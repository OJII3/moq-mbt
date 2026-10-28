#include <moonbit.h>
#include <msquic.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct event {
    struct event *next;
    int type;
    uint64_t id, code;
    HQUIC stream;
    size_t size;
    uint8_t data[];
} event;

typedef struct datagram_send {
    struct datagram_send *next;
    uint64_t id;
    QUIC_BUFFER buffer;
    uint8_t data[];
} datagram_send;

typedef struct client {
    const QUIC_API_TABLE *api;
    HQUIC registration, configuration, connection;
    pthread_mutex_t mutex;
    pthread_cond_t shutdown_complete;
    event *head, *tail, *current;
    datagram_send *pending_datagrams;
    size_t queued_bytes;
    unsigned queued_events;
    size_t max_queued_bytes;
    unsigned max_queued_events;
    uint16_t max_datagram_length;
    int datagram_send_enabled;
    int connection_started;
    int failed, closed;
} client;

static int unlink_datagram_locked(client *c, datagram_send *send) {
    datagram_send **link = &c->pending_datagrams;
    while (*link) {
        if (*link == send) {
            *link = send->next;
            return 1;
        }
        link = &(*link)->next;
    }
    return 0;
}

static void push(client *c, int type, uint64_t id, uint64_t code, HQUIC stream, const uint8_t *data, size_t size) {
    pthread_mutex_lock(&c->mutex);
    event *e = (c->failed || size > c->max_queued_bytes - c->queued_bytes || c->queued_events >= c->max_queued_events)
        ? NULL : malloc(sizeof(*e) + size);
    if (!e) {
        c->failed = 1;
        pthread_mutex_unlock(&c->mutex);
        c->api->ConnectionShutdown(c->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 1);
        return;
    }
    *e = (event){ .type = type, .id = id, .code = code, .stream = stream, .size = size };
    if (size) memcpy(e->data, data, size);
    if (c->tail) c->tail->next = e; else c->head = e;
    c->tail = e;
    c->queued_bytes += size;
    c->queued_events++;
    pthread_mutex_unlock(&c->mutex);
}

static QUIC_STATUS QUIC_API stream_cb(HQUIC s, void *ctx, QUIC_STREAM_EVENT *e) {
    client *c = ctx;
    uint64_t id = 0;
    uint32_t len = sizeof(id);
    c->api->GetParam(s, QUIC_PARAM_STREAM_ID, &len, &id);
    switch (e->Type) {
    case QUIC_STREAM_EVENT_RECEIVE:
        for (uint32_t i = 0; i < e->RECEIVE.BufferCount; i++)
            push(c, 2, id, 0, NULL, e->RECEIVE.Buffers[i].Buffer, e->RECEIVE.Buffers[i].Length);
        break;
    case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
        push(c, 3, id, 0, NULL, NULL, 0);
        break;
    case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
        push(c, 7, id, e->PEER_SEND_ABORTED.ErrorCode, NULL, NULL, 0);
        break;
    case QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED:
        push(c, 9, id, e->PEER_RECEIVE_ABORTED.ErrorCode, NULL, NULL, 0);
        break;
    case QUIC_STREAM_EVENT_SEND_COMPLETE:
        free(e->SEND_COMPLETE.ClientContext);
        push(c, 8, id, e->SEND_COMPLETE.Canceled ? 1 : 0, NULL, NULL, 0);
        break;
    case QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE:
        if (e->SEND_SHUTDOWN_COMPLETE.Graceful)
            push(c, 15, id, 0, NULL, NULL, 0);
        break;
    case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
        push(c, 10, id, 0, NULL, NULL, 0);
        break;
    default:
        break;
    }
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS QUIC_API conn_cb(HQUIC conn, void *ctx, QUIC_CONNECTION_EVENT *e) {
    client *c = ctx;
    switch (e->Type) {
    case QUIC_CONNECTION_EVENT_DATAGRAM_STATE_CHANGED:
        pthread_mutex_lock(&c->mutex);
        c->datagram_send_enabled = e->DATAGRAM_STATE_CHANGED.SendEnabled;
        c->max_datagram_length = e->DATAGRAM_STATE_CHANGED.MaxSendLength;
        pthread_mutex_unlock(&c->mutex);
        push(c, 14, e->DATAGRAM_STATE_CHANGED.SendEnabled,
             e->DATAGRAM_STATE_CHANGED.MaxSendLength, NULL, NULL, 0);
        break;
    case QUIC_CONNECTION_EVENT_DATAGRAM_RECEIVED:
        push(c, 12, 0, 0, NULL,
             e->DATAGRAM_RECEIVED.Buffer->Buffer,
             e->DATAGRAM_RECEIVED.Buffer->Length);
        break;
    case QUIC_CONNECTION_EVENT_DATAGRAM_SEND_STATE_CHANGED:
        {
            datagram_send *send = e->DATAGRAM_SEND_STATE_CHANGED.ClientContext;
            if (send) {
                push(c, 13, send->id, e->DATAGRAM_SEND_STATE_CHANGED.State, NULL, NULL, 0);
                if (QUIC_DATAGRAM_SEND_STATE_IS_FINAL(e->DATAGRAM_SEND_STATE_CHANGED.State)) {
                    pthread_mutex_lock(&c->mutex);
                    unlink_datagram_locked(c, send);
                    pthread_mutex_unlock(&c->mutex);
                    free(send);
                }
            }
        }
        break;
    case QUIC_CONNECTION_EVENT_CONNECTED: {
        push(c, 1, 0, 0, NULL, NULL, 0);
        break;
    }
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
        push(c, 5, 0, (uint32_t)e->SHUTDOWN_INITIATED_BY_TRANSPORT.Status, NULL, NULL, 0);
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
        push(c, 4, 0, e->SHUTDOWN_INITIATED_BY_PEER.ErrorCode, NULL, NULL, 0);
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE: {
        pthread_mutex_lock(&c->mutex);
        datagram_send *pending = c->pending_datagrams;
        c->pending_datagrams = NULL;
        pthread_mutex_unlock(&c->mutex);
        while (pending) {
            datagram_send *next = pending->next;
            free(pending);
            pending = next;
        }
        pthread_mutex_lock(&c->mutex);
        c->closed = 1;
        pthread_cond_broadcast(&c->shutdown_complete);
        pthread_mutex_unlock(&c->mutex);
        break;
    }
    case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED: {
        HQUIC stream = e->PEER_STREAM_STARTED.Stream;
        uint64_t id = 0;
        uint32_t len = sizeof(id);
        c->api->SetCallbackHandler(stream, (void *)stream_cb, c);
        c->api->GetParam(stream, QUIC_PARAM_STREAM_ID, &len, &id);
        push(c, 11, id, 0, stream, NULL, 0);
        break;
    }
    default:
        break;
    }
    return QUIC_STATUS_SUCCESS;
}

void moq_sleep_ms(int32_t ms) { if (ms > 0) usleep((useconds_t)ms * 1000); }
int32_t moq_client_valid(client *c) { return c != NULL; }
void moq_client_stream_close(client *c, HQUIC s) { if (c && s) c->api->StreamClose(s); }

void moq_client_shutdown(client *c, int64_t code) {
    if (c && c->connection)
        c->api->ConnectionShutdown(c->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, (uint64_t)code & QUIC_UINT62_MAX);
}

int32_t moq_client_shutdown_complete(client *c) {
    if (!c) return 1;
    pthread_mutex_lock(&c->mutex);
    int complete = c->closed;
    pthread_mutex_unlock(&c->mutex);
    return complete;
}

void moq_client_free(client *c) {
    if (!c) return;
    if (c->connection && c->connection_started) {
        pthread_mutex_lock(&c->mutex);
        int complete = c->closed;
        pthread_mutex_unlock(&c->mutex);
        if (!complete) {
            c->api->ConnectionShutdown(c->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
            pthread_mutex_lock(&c->mutex);
            while (!c->closed) pthread_cond_wait(&c->shutdown_complete, &c->mutex);
            pthread_mutex_unlock(&c->mutex);
        }
    }
    if (c->connection) c->api->ConnectionClose(c->connection);
    if (c->configuration) c->api->ConfigurationClose(c->configuration);
    if (c->registration) c->api->RegistrationClose(c->registration);
    if (c->api) MsQuicClose(c->api);
    for (event *e = c->head, *next; e; e = next) { next = e->next; free(e); }
    free(c->current);
    for (datagram_send *send = c->pending_datagrams, *next; send; send = next) {
        next = send->next;
        free(send);
    }
    pthread_cond_destroy(&c->shutdown_complete);
    pthread_mutex_destroy(&c->mutex);
    free(c);
}

client *moq_client_connect(moonbit_bytes_t host, int32_t port, moonbit_bytes_t alpn, int32_t alpn_len, int32_t insecure,
                           int32_t max_queued_bytes, int32_t max_queued_events, int32_t max_streams,
                           int32_t handshake_timeout_ms, int32_t idle_timeout_ms) {
    client *c = calloc(1, sizeof(*c));
    if (!c) return NULL;
    c->max_queued_bytes = (size_t)max_queued_bytes;
    c->max_queued_events = (unsigned)max_queued_events;
    pthread_mutex_init(&c->mutex, NULL);
    pthread_cond_init(&c->shutdown_complete, NULL);
    QUIC_REGISTRATION_CONFIG reg = { "moq-mbt", QUIC_EXECUTION_PROFILE_LOW_LATENCY };
    QUIC_BUFFER alpn_buf = { (uint32_t)alpn_len, alpn };
    QUIC_SETTINGS settings = {
        .IsSet = { .SendBufferingEnabled = 1, .DatagramReceiveEnabled = 1, .PeerBidiStreamCount = 1, .PeerUnidiStreamCount = 1, .HandshakeIdleTimeoutMs = 1, .IdleTimeoutMs = 1 },
        .SendBufferingEnabled = FALSE, .DatagramReceiveEnabled = TRUE, .PeerBidiStreamCount = (uint16_t)max_streams, .PeerUnidiStreamCount = (uint16_t)max_streams, .HandshakeIdleTimeoutMs = (uint64_t)handshake_timeout_ms, .IdleTimeoutMs = (uint64_t)idle_timeout_ms,
    };
    QUIC_CREDENTIAL_CONFIG cred = { .Type = QUIC_CREDENTIAL_TYPE_NONE, .Flags = QUIC_CREDENTIAL_FLAG_CLIENT | (insecure ? QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION : 0) };
    if (QUIC_FAILED(MsQuicOpen2(&c->api)) ||
        QUIC_FAILED(c->api->RegistrationOpen(&reg, &c->registration)) ||
        QUIC_FAILED(c->api->ConfigurationOpen(c->registration, &alpn_buf, 1, &settings, sizeof(settings), NULL, &c->configuration)) ||
        QUIC_FAILED(c->api->ConfigurationLoadCredential(c->configuration, &cred)) ||
        QUIC_FAILED(c->api->ConnectionOpen(c->registration, conn_cb, c, &c->connection))) {
        moq_client_free(c);
        return NULL;
    }
    QUIC_STATUS status = c->api->ConnectionStart(c->connection, c->configuration, QUIC_ADDRESS_FAMILY_UNSPEC, (const char *)host, (uint16_t)port);
    if (QUIC_FAILED(status)) {
        moq_client_free(c);
        return NULL;
    }
    c->connection_started = 1;
    return c;
}

HQUIC moq_client_open_uni(client *c) {
    HQUIC s = NULL;
    if (QUIC_FAILED(c->api->StreamOpen(c->connection, QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL, stream_cb, c, &s))) return NULL;
    if (QUIC_FAILED(c->api->StreamStart(s, QUIC_STREAM_START_FLAG_IMMEDIATE))) { c->api->StreamClose(s); return NULL; }
    return s;
}

HQUIC moq_client_open_bidi(client *c) {
    HQUIC s = NULL;
    if (QUIC_FAILED(c->api->StreamOpen(c->connection, QUIC_STREAM_OPEN_FLAG_NONE, stream_cb, c, &s))) return NULL;
    if (QUIC_FAILED(c->api->StreamStart(s, QUIC_STREAM_START_FLAG_IMMEDIATE))) { c->api->StreamClose(s); return NULL; }
    return s;
}

int64_t moq_client_stream_id(client *c, HQUIC s) {
    uint64_t id = 0;
    uint32_t len = sizeof(id);
    return (!s || QUIC_FAILED(c->api->GetParam(s, QUIC_PARAM_STREAM_ID, &len, &id))) ? -1 : (int64_t)id;
}

int32_t moq_client_send(client *c, HQUIC s, moonbit_bytes_t data, int32_t len, int32_t fin) {
    QUIC_BUFFER *buf = malloc(sizeof(*buf) + (size_t)len);
    if (!buf) return -1;
    *buf = (QUIC_BUFFER){ (uint32_t)len, (uint8_t *)(buf + 1) };
    if (len) memcpy(buf->Buffer, data, (size_t)len);
    QUIC_STATUS status = c->api->StreamSend(s, buf, 1, fin ? QUIC_SEND_FLAG_FIN : QUIC_SEND_FLAG_NONE, buf);
    if (QUIC_FAILED(status)) { free(buf); return (int32_t)status; }
    return 0;
}

int32_t moq_client_abort_send(client *c, HQUIC s, int64_t error_code) {
    QUIC_STATUS status = c->api->StreamShutdown(
        s, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND, (uint64_t)error_code);
    return QUIC_FAILED(status) ? (int32_t)status : 0;
}

int32_t moq_client_stop_receiving(client *c, HQUIC s, int64_t error_code) {
    QUIC_STATUS status = c->api->StreamShutdown(
        s, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, (uint64_t)error_code);
    return QUIC_FAILED(status) ? (int32_t)status : 0;
}

int32_t moq_client_send_datagram(client *c, moonbit_bytes_t data, int32_t len, int64_t id) {
    if (len < 0) return -1;
    pthread_mutex_lock(&c->mutex);
    int enabled = c->datagram_send_enabled;
    uint16_t max_length = c->max_datagram_length;
    pthread_mutex_unlock(&c->mutex);
    if (!enabled) return 1;
    if ((uint32_t)len > max_length) return 2;

    datagram_send *send = malloc(sizeof(*send) + (size_t)len);
    if (!send) return 3;
    send->id = (uint64_t)id;
    send->buffer = (QUIC_BUFFER){ (uint32_t)len, send->data };
    if (len) memcpy(send->data, data, (size_t)len);
    pthread_mutex_lock(&c->mutex);
    send->next = c->pending_datagrams;
    c->pending_datagrams = send;
    pthread_mutex_unlock(&c->mutex);
    QUIC_STATUS status = c->api->DatagramSend(c->connection, &send->buffer, 1,
                                               QUIC_SEND_FLAG_NONE, send);
    if (QUIC_FAILED(status)) {
        pthread_mutex_lock(&c->mutex);
        unlink_datagram_locked(c, send);
        pthread_mutex_unlock(&c->mutex);
        free(send);
        return (int32_t)status;
    }
    return 0;
}

int64_t moq_client_datagram_state(client *c) {
    pthread_mutex_lock(&c->mutex);
    int64_t state = ((int64_t)c->max_datagram_length << 1) | (c->datagram_send_enabled ? 1 : 0);
    pthread_mutex_unlock(&c->mutex);
    return state;
}

int32_t moq_client_pop(client *c) {
    free(c->current);
    c->current = NULL;
    pthread_mutex_lock(&c->mutex);
    if (c->failed) { pthread_mutex_unlock(&c->mutex); return 6; }
    event *e = c->head;
    if (e) {
        c->head = e->next;
        if (!c->head) c->tail = NULL;
        c->queued_bytes -= e->size;
        c->queued_events--;
        c->current = e;
    }
    int type = e ? e->type : (c->closed ? 4 : 0);
    pthread_mutex_unlock(&c->mutex);
    return type;
}

int64_t moq_client_event_id(client *c) { return c->current ? (int64_t)c->current->id : 0; }
int64_t moq_client_event_code(client *c) { return c->current ? (int64_t)c->current->code : 0; }
HQUIC moq_client_event_stream(client *c) { return c->current ? c->current->stream : NULL; }
moonbit_bytes_t moq_client_event_data(client *c) {
    event *e = c->current;
    moonbit_bytes_t data = moonbit_make_bytes_raw((int32_t)e->size);
    if (e->size) memcpy(data, e->data, e->size);
    return data;
}
