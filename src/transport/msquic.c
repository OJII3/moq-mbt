/* MsQuic callbacks use only C allocations. MoonBit allocations happen in poll. */
#include <moonbit.h>
#include <msquic.h>
#include <pthread.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_BYTES (1024 * 1024)
#define MAX_EVENTS 256
#define MAX_STREAMS 16

typedef struct client client;
typedef struct stream {
    client *owner;
    HQUIC handle;
    uint64_t id;
    struct stream *next;
} stream;
typedef struct event {
    struct event *next;
    int type;
    uint64_t id;
    size_t size;
    uint8_t data[];
} event;
typedef struct send_buffer {
    QUIC_BUFFER buffer;
    uint8_t data[];
} send_buffer;
struct client {
    const QUIC_API_TABLE *api;
    HQUIC registration, configuration, connection;
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    event *head, *tail, *current;
    size_t queued_bytes, pending_send_bytes;
    unsigned queued_events, stream_count, pending_sends;
    stream *streams;
    int failed, closed, transport_closed;
    uint64_t error;
};

/* Called with no client mutex held. Queue overflow is reported as QueueOverflow. */
static int enqueue(client *c, int type, uint64_t id, const uint8_t *data, size_t size) {
    pthread_mutex_lock(&c->mutex);
    if (c->failed || size > MAX_BYTES - c->queued_bytes || c->queued_events >= MAX_EVENTS) {
        c->failed = 1;
        pthread_cond_signal(&c->condition);
        pthread_mutex_unlock(&c->mutex);
        return 0;
    }
    event *e = malloc(sizeof(*e) + size);
    if (!e) {
        c->failed = 1;
        pthread_cond_signal(&c->condition);
        pthread_mutex_unlock(&c->mutex);
        return 0;
    }
    *e = (event){ .type = type, .id = id, .size = size };
    if (size) memcpy(e->data, data, size);
    if (c->tail) c->tail->next = e; else c->head = e;
    c->tail = e;
    c->queued_bytes += size;
    c->queued_events++;
    pthread_cond_signal(&c->condition);
    pthread_mutex_unlock(&c->mutex);
    return 1;
}

static QUIC_STATUS QUIC_API stream_callback(HQUIC handle, void *context, QUIC_STREAM_EVENT *e) {
    (void)handle;
    stream *s = context;
    client *c = s->owner;
    switch (e->Type) {
    case QUIC_STREAM_EVENT_RECEIVE:
        for (uint32_t i = 0; i < e->RECEIVE.BufferCount; i++) {
            QUIC_BUFFER *b = (QUIC_BUFFER *)&e->RECEIVE.Buffers[i];
            if (!enqueue(c, 2, s->id, b->Buffer, b->Length)) {
                c->api->ConnectionShutdown(c->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 1);
                break;
            }
        }
        break;
    case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
        if (!enqueue(c, 3, s->id, NULL, 0))
            c->api->ConnectionShutdown(c->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 1);
        break;
    case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
        if (!enqueue(c, 7, s->id, (const uint8_t *)&e->PEER_SEND_ABORTED.ErrorCode, sizeof(uint64_t)))
            c->api->ConnectionShutdown(c->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 1);
        break;
    case QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED:
        if (!enqueue(c, 9, s->id, (const uint8_t *)&e->PEER_RECEIVE_ABORTED.ErrorCode, sizeof(uint64_t)))
            c->api->ConnectionShutdown(c->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 1);
        break;
    case QUIC_STREAM_EVENT_SEND_COMPLETE: {
        uint64_t canceled = e->SEND_COMPLETE.Canceled ? 1 : 0;
        pthread_mutex_lock(&c->mutex);
        c->pending_send_bytes -= ((send_buffer *)e->SEND_COMPLETE.ClientContext)->buffer.Length;
        c->pending_sends--;
        pthread_mutex_unlock(&c->mutex);
        free(e->SEND_COMPLETE.ClientContext);
        if (!enqueue(c, 8, s->id, (const uint8_t *)&canceled, sizeof(canceled)))
            c->api->ConnectionShutdown(c->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 1);
        break;
    }
    default:
        break;
    }
    return QUIC_STATUS_SUCCESS;
}

static stream *add_stream(client *c, HQUIC handle) {
    stream *s = calloc(1, sizeof(*s));
    if (!s) return NULL;
    s->owner = c;
    s->handle = handle;
    uint32_t size = sizeof(s->id);
    if (QUIC_FAILED(c->api->GetParam(handle, QUIC_PARAM_STREAM_ID, &size, &s->id))) {
        free(s);
        return NULL;
    }
    pthread_mutex_lock(&c->mutex);
    if (c->stream_count >= MAX_STREAMS) {
        pthread_mutex_unlock(&c->mutex);
        free(s);
        return NULL;
    }
    s->next = c->streams;
    c->streams = s;
    c->stream_count++;
    pthread_mutex_unlock(&c->mutex);
    return s;
}

static QUIC_STATUS QUIC_API connection_callback(HQUIC handle, void *context, QUIC_CONNECTION_EVENT *e) {
    client *c = context;
    switch (e->Type) {
    case QUIC_CONNECTION_EVENT_CONNECTED: {
        BOOLEAN datagram_enabled = FALSE;
        uint32_t size = sizeof(datagram_enabled);
        QUIC_STATUS status = c->api->GetParam(handle, QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED, &size, &datagram_enabled);
        if (QUIC_FAILED(status) || !datagram_enabled) {
            pthread_mutex_lock(&c->mutex);
            c->error = QUIC_STATUS_NOT_SUPPORTED;
            c->transport_closed = 1;
            pthread_mutex_unlock(&c->mutex);
            c->api->ConnectionShutdown(handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 3);
            break;
        }
        if (!enqueue(c, 1, 0, NULL, 0))
            c->api->ConnectionShutdown(handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 1);
        break;
    }
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
        pthread_mutex_lock(&c->mutex);
        c->error = (uint64_t)e->SHUTDOWN_INITIATED_BY_TRANSPORT.Status;
        c->transport_closed = 1;
        pthread_mutex_unlock(&c->mutex);
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
        pthread_mutex_lock(&c->mutex);
        c->error = e->SHUTDOWN_INITIATED_BY_PEER.ErrorCode;
        pthread_mutex_unlock(&c->mutex);
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
        pthread_mutex_lock(&c->mutex);
        c->closed = 1;
        pthread_cond_signal(&c->condition);
        pthread_mutex_unlock(&c->mutex);
        break;
    case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED: {
        stream *s = add_stream(c, e->PEER_STREAM_STARTED.Stream);
        if (!s) {
            c->api->StreamClose(e->PEER_STREAM_STARTED.Stream);
            c->api->ConnectionShutdown(handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 1);
        } else {
            c->api->SetCallbackHandler(s->handle, (void *)stream_callback, s);
        }
        break;
    }
    default:
        break;
    }
    return QUIC_STATUS_SUCCESS;
}

void moq_client_close(client *c, int64_t error_code) {
    if (!c) return;
    /* Signal graceful connection shutdown, then wait a bounded time. */
    if (c->connection) {
        c->api->ConnectionShutdown(c->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, (uint64_t)error_code & QUIC_UINT62_MAX);
        struct timespec until;
        clock_gettime(CLOCK_REALTIME, &until);
        until.tv_sec += 3;
        pthread_mutex_lock(&c->mutex);
        while (!c->closed) {
            if (pthread_cond_timedwait(&c->condition, &c->mutex, &until)) break;
        }
        pthread_mutex_unlock(&c->mutex);
        /* Blocks until callbacks end; stream contexts stay alive until then. */
        c->api->ConnectionClose(c->connection);
    }
    stream *s = c->streams;
    while (s) {
        stream *next = s->next;
        c->api->StreamClose(s->handle);
        free(s);
        s = next;
    }
    if (c->configuration) c->api->ConfigurationClose(c->configuration);
    if (c->registration) c->api->RegistrationClose(c->registration);
    if (c->api) MsQuicClose(c->api);
    event *e = c->head;
    while (e) {
        event *next = e->next;
        free(e);
        e = next;
    }
    free(c->current);
    pthread_cond_destroy(&c->condition);
    pthread_mutex_destroy(&c->mutex);
    free(c);
}

client *moq_client_connect(moonbit_bytes_t host, int32_t length, int32_t port, int32_t insecure) {
    /* Host is copied because ConnectionStart expects a zero terminated name. */
    if (length <= 0 || length > 65535 || memchr(host, 0, length)) return NULL;
    char *name = malloc((size_t)length + 1);
    client *c = calloc(1, sizeof(*c));
    if (!name || !c) { free(name); free(c); return NULL; }
    memcpy(name, host, length);
    name[length] = 0;
    pthread_mutex_init(&c->mutex, NULL);
    pthread_cond_init(&c->condition, NULL);
    QUIC_REGISTRATION_CONFIG registration = { "moq-mbt", QUIC_EXECUTION_PROFILE_LOW_LATENCY };
    uint8_t alpn_data[] = "moqt-18";
    QUIC_BUFFER alpn = { sizeof(alpn_data) - 1, alpn_data };
    QUIC_SETTINGS settings = {0};
    /* With buffering disabled, noncanceled completion follows peer ACKs. */
    settings.IsSet.SendBufferingEnabled = TRUE;
    settings.SendBufferingEnabled = FALSE;
    settings.IsSet.DatagramReceiveEnabled = TRUE;
    settings.DatagramReceiveEnabled = TRUE;
    settings.IsSet.PeerUnidiStreamCount = TRUE;
    settings.PeerUnidiStreamCount = MAX_STREAMS;
    settings.IsSet.HandshakeIdleTimeoutMs = TRUE;
    settings.HandshakeIdleTimeoutMs = 10000;
    settings.IsSet.IdleTimeoutMs = TRUE;
    settings.IdleTimeoutMs = 30000;
    QUIC_CREDENTIAL_CONFIG credentials = {0};
    credentials.Type = QUIC_CREDENTIAL_TYPE_NONE;
    credentials.Flags = QUIC_CREDENTIAL_FLAG_CLIENT;
    if (insecure) credentials.Flags |= QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION;
    QUIC_STATUS status = MsQuicOpen2(&c->api);
    if (QUIC_FAILED(status)) goto fail;
    status = c->api->RegistrationOpen(&registration, &c->registration);
    if (QUIC_FAILED(status)) goto fail;
    status = c->api->ConfigurationOpen(c->registration, &alpn, 1, &settings, sizeof(settings), NULL, &c->configuration);
    if (QUIC_FAILED(status)) goto fail;
    status = c->api->ConfigurationLoadCredential(c->configuration, &credentials);
    if (QUIC_FAILED(status)) goto fail;
    status = c->api->ConnectionOpen(c->registration, connection_callback, c, &c->connection);
    if (QUIC_FAILED(status)) goto fail;
    status = c->api->ConnectionStart(c->connection, c->configuration, QUIC_ADDRESS_FAMILY_UNSPEC, name, (uint16_t)port);
    if (QUIC_FAILED(status)) goto fail;
    free(name);
    return c;
fail:
    free(name);
    moq_client_close(c, 0);
    return NULL;
}

int32_t moq_client_valid(client *c) { return c != NULL; }

int64_t moq_client_open_uni(client *c) {
    HQUIC handle;
    /* Stream callbacks cannot fire until StreamStart/StreamSend. */
    QUIC_STATUS status = c->api->StreamOpen(c->connection, QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL, stream_callback, NULL, &handle);
    if (QUIC_FAILED(status)) return -1;
    /* Start synchronously assigns the stream ID; delay callbacks until context set. */
    stream *s = calloc(1, sizeof(*s));
    if (!s) { c->api->StreamClose(handle); return -1; }
    s->owner = c;
    s->handle = handle;
    c->api->SetContext(handle, s);
    status = c->api->StreamStart(handle, QUIC_STREAM_START_FLAG_IMMEDIATE);
    if (QUIC_FAILED(status)) { c->api->StreamClose(handle); free(s); return -1; }
    uint32_t size = sizeof(s->id);
    status = c->api->GetParam(handle, QUIC_PARAM_STREAM_ID, &size, &s->id);
    if (QUIC_FAILED(status)) { c->api->StreamClose(handle); free(s); return -1; }
    pthread_mutex_lock(&c->mutex);
    if (c->stream_count >= MAX_STREAMS) {
        pthread_mutex_unlock(&c->mutex);
        c->api->StreamClose(handle);
        free(s);
        return -1;
    }
    s->next = c->streams;
    c->streams = s;
    c->stream_count++;
    pthread_mutex_unlock(&c->mutex);
    return (int64_t)s->id;
}

int32_t moq_client_send(client *c, int64_t id, moonbit_bytes_t data, int32_t length, int32_t fin) {
    if (length < 0 || length > MAX_BYTES) return -1;
    pthread_mutex_lock(&c->mutex);
    stream *s = c->streams;
    while (s && s->id != (uint64_t)id) s = s->next;
    pthread_mutex_unlock(&c->mutex);
    if (!s) return -1;
    send_buffer *send = malloc(sizeof(*send) + (size_t)length);
    if (!send) return -1;
    send->buffer = (QUIC_BUFFER){ (uint32_t)length, send->data };
    if (length) memcpy(send->data, data, length);
    pthread_mutex_lock(&c->mutex);
    if ((size_t)length > MAX_BYTES - c->pending_send_bytes || c->pending_sends >= MAX_EVENTS) {
        pthread_mutex_unlock(&c->mutex);
        free(send);
        return -1;
    }
    c->pending_send_bytes += length;
    c->pending_sends++;
    pthread_mutex_unlock(&c->mutex);
    QUIC_STATUS status = c->api->StreamSend(s->handle, &send->buffer, 1, fin ? QUIC_SEND_FLAG_FIN : QUIC_SEND_FLAG_NONE, send);
    if (QUIC_FAILED(status)) {
        pthread_mutex_lock(&c->mutex);
        c->pending_send_bytes -= length;
        c->pending_sends--;
        pthread_mutex_unlock(&c->mutex);
        free(send);
    }
    return QUIC_FAILED(status) ? (int32_t)status : 0;
}

int32_t moq_client_next(client *c, int32_t timeout) {
    free(c->current);
    c->current = NULL;
    struct timespec until;
    clock_gettime(CLOCK_REALTIME, &until);
    until.tv_sec += timeout / 1000;
    until.tv_nsec += (timeout % 1000) * 1000000L;
    if (until.tv_nsec >= 1000000000L) { until.tv_sec++; until.tv_nsec -= 1000000000L; }
    pthread_mutex_lock(&c->mutex);
    while (!c->head && !c->closed && !c->failed) {
        int result = pthread_cond_timedwait(&c->condition, &c->mutex, &until);
        if (result == ETIMEDOUT) { pthread_mutex_unlock(&c->mutex); return 0; }
        if (result) { pthread_mutex_unlock(&c->mutex); return -1; }
    }
    if (c->failed) { pthread_mutex_unlock(&c->mutex); return 6; }
    event *e = c->head;
    if (!e) {
        int type = c->transport_closed ? 5 : 4;
        pthread_mutex_unlock(&c->mutex);
        return type;
    }
    c->head = e->next;
    if (!c->head) c->tail = NULL;
    c->queued_bytes -= e->size;
    c->queued_events--;
    c->current = e;
    pthread_mutex_unlock(&c->mutex);
    return e->type;
}

int64_t moq_client_event_id(client *c) {
    if (c->current) return (int64_t)c->current->id;
    pthread_mutex_lock(&c->mutex);
    int64_t id = c->failed ? -1 : (int64_t)c->error;
    pthread_mutex_unlock(&c->mutex);
    return id;
}

moonbit_bytes_t moq_client_event_data(client *c) {
    event *e = c->current;
    moonbit_bytes_t data = moonbit_make_bytes_raw((int32_t)e->size);
    if (e->size) memcpy(data, e->data, e->size);
    return data;
}

int64_t moq_client_event_code(client *c) {
    uint64_t code;
    memcpy(&code, c->current->data, sizeof(code));
    return (int64_t)code;
}
