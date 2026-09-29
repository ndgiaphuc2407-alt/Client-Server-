#include <animate/animate.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_LINE     1024
#define MAX_USERNAME 33
#define MAX_CLIENTS  64
#define MAX_HANDLES  256
#define MAX_BARRIERS 64
#define USERS_FILE   "users.txt"

/* ── Handle ──────────────────────────────────────────────────────── */
typedef enum { HT_CANVAS, HT_SPRITE, HT_PLACEMENT } htype_t;
typedef struct { uint64_t id; void *ptr; htype_t type; } handle_t;

/* ── Client ──────────────────────────────────────────────────────── */
typedef enum { CS_PENDING, CS_AUTH, CS_DEAD } cstate_t;
typedef struct {
    pid_t    pid;
    char     username[MAX_USERNAME];
    int      fd_r, fd_w;
    char     path_c2s[64], path_s2c[64];
    cstate_t state;
    handle_t handles[MAX_HANDLES];
    int      nhandles;
    pthread_mutex_t hmtx;
} client_t;

/* ── Thread-pool task ────────────────────────────────────────────── */
typedef struct task {
    client_t   *client;
    char        cmd[MAX_LINE];
    struct task *next;
} task_t;

typedef struct {
    pthread_t      *workers;
    int             nworkers;
    task_t         *head, *tail;
    pthread_mutex_t mtx;
    pthread_cond_t  cv;
    int             shutdown;
} pool_t;

/* ── Barrier entry ───────────────────────────────────────────────── */
typedef struct {
    uint64_t canvas_id;
    int      arrived;
    int      total;
    int      in_use;
} barrier_entry_t;

/* ── Server state ────────────────────────────────────────────────── */
typedef struct {
    client_t       *clients[MAX_CLIENTS];
    int             nclient;
    pthread_mutex_t cmtx;
    pool_t          pool;
    uint64_t        next_id;
    pthread_mutex_t id_mtx;
    int             sigpipe[2];
    int             alive;
    barrier_entry_t barriers[MAX_BARRIERS];
    pthread_mutex_t bar_mtx;
    pthread_cond_t  bar_cv;
} srv_t;

static int g_pipe_w = -1;

/* ── Signal handlers ─────────────────────────────────────────────── */
static void on_sigusr1(int s, siginfo_t *si, void *u)
{
    (void)s; (void)u;
    pid_t p = si->si_pid;
    (void)write(g_pipe_w, &p, sizeof p);
}

static void on_sigterm(int s)
{
    (void)s;
    pid_t z = 0;
    (void)write(g_pipe_w, &z, sizeof z);
}

#define WAKEUP_PID ((pid_t)-1)

static void mark_client_dead(srv_t *s, client_t *c)
{
    pthread_mutex_lock(&s->cmtx);
    c->state = CS_DEAD;
    pthread_mutex_unlock(&s->cmtx);
    pid_t wake = WAKEUP_PID;
    (void)write(g_pipe_w, &wake, sizeof wake);
}

/* ── Handle helpers ──────────────────────────────────────────────── */
static uint64_t hadd(srv_t *s, client_t *c, void *p, htype_t t)
{
    pthread_mutex_lock(&c->hmtx);
    if (c->nhandles >= MAX_HANDLES) {
        pthread_mutex_unlock(&c->hmtx);
        return 0;
    }
    pthread_mutex_lock(&s->id_mtx);
    uint64_t id = ++s->next_id;
    pthread_mutex_unlock(&s->id_mtx);
    c->handles[c->nhandles++] = (handle_t){ id, p, t };
    pthread_mutex_unlock(&c->hmtx);
    return id;
}

static void *hlook(client_t *c, uint64_t id, htype_t t)
{
    pthread_mutex_lock(&c->hmtx);
    for (int i = 0; i < c->nhandles; i++) {
        if (c->handles[i].id == id && c->handles[i].type == t) {
            void *p = c->handles[i].ptr;
            pthread_mutex_unlock(&c->hmtx);
            return p;
        }
    }
    pthread_mutex_unlock(&c->hmtx);
    return NULL;
}

static void hrem(client_t *c, uint64_t id)
{
    pthread_mutex_lock(&c->hmtx);
    for (int i = 0; i < c->nhandles; i++) {
        if (c->handles[i].id == id) {
            c->handles[i] = c->handles[--c->nhandles];
            break;
        }
    }
    pthread_mutex_unlock(&c->hmtx);
}

/* Insert a handle using an existing ID (for sharing); idempotent if already present */
static int hadd_existing(client_t *c, void *p, htype_t t, uint64_t id)
{
    pthread_mutex_lock(&c->hmtx);
    for (int i = 0; i < c->nhandles; i++) {
        if (c->handles[i].id == id) {
            pthread_mutex_unlock(&c->hmtx);
            return 0;
        }
    }
    if (c->nhandles >= MAX_HANDLES) { pthread_mutex_unlock(&c->hmtx); return -1; }
    c->handles[c->nhandles++] = (handle_t){ id, p, t };
    pthread_mutex_unlock(&c->hmtx);
    return 0;
}

/* Find an authenticated client by username (caller must hold cmtx) */
static client_t *find_client_by_name(srv_t *s, const char *name)
{
    for (int i = 0; i < s->nclient; i++)
        if (s->clients[i]->state == CS_AUTH &&
            strcmp(s->clients[i]->username, name) == 0)
            return s->clients[i];
    return NULL;
}

/* ── users.txt lookup ────────────────────────────────────────────── */
static int users_lookup(const char *name, int *bal)
{
    FILE *f = fopen(USERS_FILE, "r");
    if (!f) return 0;
    char line[256], u[MAX_USERNAME];
    int  b;
    while (fgets(line, sizeof line, f)) {
        if (sscanf(line, " %32s %d", u, &b) == 2 && strcmp(u, name) == 0) {
            *bal = b;
            fclose(f);
            return 1;
        }
    }
    fclose(f);
    return 0;
}

static void wstr(int fd, const char *s)
{
    (void)write(fd, s, strlen(s));
}

/* ── Q2:
 * ──────────────────────────────────────────────────────────────────── */
static void canvas_safe_destroy(srv_t *s, uint64_t canvas_id, void *cv)
{
    pthread_mutex_lock(&s->cmtx);
    int still_shared = 0;
    for (int i = 0; i < s->nclient; i++) {
        client_t *ci = s->clients[i];
        if (ci->state == CS_DEAD) continue;
        pthread_mutex_lock(&ci->hmtx);
        for (int j = 0; j < ci->nhandles; j++) {
            if (ci->handles[j].id == canvas_id &&
                ci->handles[j].type == HT_CANVAS) {
                still_shared = 1;
                break;
            }
        }
        pthread_mutex_unlock(&ci->hmtx);
        if (still_shared) break;
    }
    pthread_mutex_unlock(&s->cmtx);

    if (!still_shared)
        animate_destroy_canvas(cv);
}

/* Q2:
 * ──────────────────────────────────────────────────────────────────── */
static void client_release_canvases(srv_t *s, client_t *c)
{
    /* Collect canvas handles without holding hmtx across the destroy call */
    uint64_t ids[MAX_HANDLES];
    void    *ptrs[MAX_HANDLES];
    int      count = 0;

    pthread_mutex_lock(&c->hmtx);
    for (int i = 0; i < c->nhandles; i++) {
        if (c->handles[i].type == HT_CANVAS) {
            ids[count]  = c->handles[i].id;
            ptrs[count] = c->handles[i].ptr;
            count++;
        }
    }
    pthread_mutex_unlock(&c->hmtx);

    for (int i = 0; i < count; i++) {
        /* Remove from this client first so canvas_safe_destroy does not
         * count it as an active reference when scanning other clients. */
        hrem(c, ids[i]);
        canvas_safe_destroy(s, ids[i], ptrs[i]);
    }
}

/* ── RPC dispatch ── */
static void dispatch(srv_t *s, client_t *c, char *cmd)
{
    char *save;
    cmd[strcspn(cmd, "\r\n")] = '\0';
    char *t = strtok_r(cmd, " ", &save);
    if (!t) { wstr(c->fd_w, "-1\n"); return; }

    if (!strcmp(t, "Login")) {
        char *u = strtok_r(NULL, " ", &save);
        if (!u) { wstr(c->fd_w, "-1\n"); return; }
        int bal;
        if (!users_lookup(u, &bal)) {
            wstr(c->fd_w, "Reject UNAUTHORISED\n");
            sleep(1);
            c->state = CS_DEAD;
            return;
        }
        if (bal <= 0) {
            wstr(c->fd_w, "Reject BALANCE\n");
            sleep(1);
            c->state = CS_DEAD;
            return;
        }
        strncpy(c->username, u, MAX_USERNAME - 1);
        c->username[MAX_USERNAME - 1] = '\0';
        c->state = CS_AUTH;
        char r[32];
        snprintf(r, sizeof r, "%d\n", bal);
        wstr(c->fd_w, r);
        return;
    }

    if (c->state != CS_AUTH) { wstr(c->fd_w, "-1\n"); return; }

    if (!strcmp(t, "Disconnect")) {
        wstr(c->fd_w, "0\n");
        mark_client_dead(s, c);
        return;
    }

    if (!strcmp(t, "generate")) {
        char *si = strtok_r(NULL, " ", &save);
        char *fn = strtok_r(NULL, " ", &save);
        char *sx = strtok_r(NULL, " ", &save);
        char *sy = strtok_r(NULL, " ", &save);
        char *sf = strtok_r(NULL, " ", &save);
        if (!si || !fn || !sx || !sy || !sf) { wstr(c->fd_w, "-1\n"); return; }
        struct canvas *cv = hlook(c, (uint64_t)strtoull(si, NULL, 10), HT_CANVAS);
        if (!cv) { wstr(c->fd_w, "-1\n"); return; }

        size_t frame_count = (size_t)strtoul(sf, NULL, 10);
        size_t frame_size  = animate_frame_size_bytes(cv);
        if (frame_size == 0) { wstr(c->fd_w, "-1\n"); return; }

        void *frame_buf = malloc(frame_size);
        if (!frame_buf) { wstr(c->fd_w, "-1\n"); return; }

        char dat_path[256], mp4_path[256], log_path[256];
        snprintf(dat_path, sizeof dat_path, "%s.dat", fn);
        snprintf(mp4_path, sizeof mp4_path, "%s.mp4", fn);
        snprintf(log_path, sizeof log_path, "%s.log", fn);

        FILE *dat = fopen(dat_path, "wb");
        FILE *log = fopen(log_path, "w");
        if (!dat || !log) {
            free(frame_buf);
            if (dat) fclose(dat);
            if (log) fclose(log);
            wstr(c->fd_w, "-1\n");
            return;
        }

        for (size_t i = 0; i < frame_count; i++) {
            animate_generate_frame(cv, i, 30, frame_buf);
            fwrite(frame_buf, frame_size, 1, dat);
            fprintf(log, "Frame %zu generated\n", i);
        }

        fclose(dat);
        fclose(log);

        FILE *mp4 = fopen(mp4_path, "wb");
        if (mp4) { fprintf(mp4, "MP4 stub\n"); fclose(mp4); }

        free(frame_buf);
        wstr(c->fd_w, "0 0 0\n");
        return;
    }

    if (!strcmp(t, "share_canvas")) {
        char *si = strtok_r(NULL, " ", &save);
        char *su = strtok_r(NULL, " ", &save);
        if (!si || !su) { wstr(c->fd_w, "-1\n"); return; }
        uint64_t id = (uint64_t)strtoull(si, NULL, 10);
        struct canvas *cv = hlook(c, id, HT_CANVAS);
        if (!cv) { wstr(c->fd_w, "-1\n"); return; }
        pthread_mutex_lock(&s->cmtx);
        client_t *tgt = find_client_by_name(s, su);
        pthread_mutex_unlock(&s->cmtx);
        if (!tgt) { wstr(c->fd_w, "-1\n"); return; }
        if (hadd_existing(tgt, cv, HT_CANVAS, id) != 0) { wstr(c->fd_w, "-1\n"); return; }
        wstr(c->fd_w, "0\n");
        return;
    }

    if (!strcmp(t, "barrier")) {
        char *si = strtok_r(NULL, " ", &save);
        if (!si) { wstr(c->fd_w, "-1\n"); return; }
        uint64_t canvas_id = (uint64_t)strtoull(si, NULL, 10);

        /* Caller must hold the canvas */
        if (!hlook(c, canvas_id, HT_CANVAS)) { wstr(c->fd_w, "-1\n"); return; }

        pthread_mutex_lock(&s->bar_mtx);

        /* Count how many alive+auth clients share this canvas */
        pthread_mutex_lock(&s->cmtx);
        int total = 0;
        for (int i = 0; i < s->nclient; i++) {
            client_t *ci = s->clients[i];
            if (ci->state != CS_AUTH) continue;
            pthread_mutex_lock(&ci->hmtx);
            for (int j = 0; j < ci->nhandles; j++) {
                if (ci->handles[j].id == canvas_id &&
                    ci->handles[j].type == HT_CANVAS) { total++; break; }
            }
            pthread_mutex_unlock(&ci->hmtx);
        }
        pthread_mutex_unlock(&s->cmtx);

        /* Find an existing barrier for this canvas, or allocate a free slot */
        barrier_entry_t *be = NULL;
        for (int i = 0; i < MAX_BARRIERS; i++) {
            if (s->barriers[i].in_use && s->barriers[i].canvas_id == canvas_id) {
                be = &s->barriers[i]; break;
            }
        }
        if (!be) {
            for (int i = 0; i < MAX_BARRIERS; i++) {
                if (!s->barriers[i].in_use) {
                    be = &s->barriers[i];
                    be->canvas_id = canvas_id;
                    be->arrived   = 0;
                    be->total     = total;
                    be->in_use    = 1;
                    break;
                }
            }
        }
        if (!be) { pthread_mutex_unlock(&s->bar_mtx); wstr(c->fd_w, "-1\n"); return; }

        be->arrived++;

        if (be->arrived >= be->total) {
            /* Last participant — release everyone */
            be->in_use = 0;
            pthread_cond_broadcast(&s->bar_cv);
            pthread_mutex_unlock(&s->bar_mtx);
        } else {
            /* Wait under bar_mtx until the last participant broadcasts */
            while (be->in_use)
                pthread_cond_wait(&s->bar_cv, &s->bar_mtx);
            pthread_mutex_unlock(&s->bar_mtx);
        }

        wstr(c->fd_w, "0\n");
        return;
    }

    if (!strcmp(t, "create_canvas")) {
        char *sh = strtok_r(NULL, " ", &save);
        char *sw = strtok_r(NULL, " ", &save);
        char *sc = strtok_r(NULL, " ", &save);
        if (!sh || !sw || !sc) { wstr(c->fd_w, "-1\n"); return; }
        long h = strtol(sh, NULL, 10);
        long w = strtol(sw, NULL, 10);
        if (h <= 0 || w <= 0) { wstr(c->fd_w, "-2\n"); return; }
        struct canvas *cv = animate_create_canvas((size_t)h, (size_t)w, (color_t)strtoul(sc, NULL, 0));
        if (!cv) { wstr(c->fd_w, "-1\n"); return; }
        char r[64];
        snprintf(r, sizeof r, "0 %"PRIu64"\n", hadd(s, c, cv, HT_CANVAS));
        wstr(c->fd_w, r);
        return;
    }

    if (!strcmp(t, "create_rectangle")) {
        char *sw = strtok_r(NULL, " ", &save);
        char *sh = strtok_r(NULL, " ", &save);
        char *sc = strtok_r(NULL, " ", &save);
        char *sf = strtok_r(NULL, " ", &save);
        if (!sw || !sh || !sc || !sf) { wstr(c->fd_w, "-1\n"); return; }
        long w = strtol(sw, NULL, 10);
        long h = strtol(sh, NULL, 10);
        long fi = strtol(sf, NULL, 10);
        if (w <= 0 || h <= 0) { wstr(c->fd_w, "-2\n"); return; }
        struct sprite *sp = animate_create_rectangle((size_t)w, (size_t)h, (color_t)strtoul(sc, NULL, 0), (bool)fi);
        if (!sp) { wstr(c->fd_w, "-1\n"); return; }
        char r[64];
        snprintf(r, sizeof r, "0 %"PRIu64"\n", hadd(s, c, sp, HT_SPRITE));
        wstr(c->fd_w, r);
        return;
    }

    if (!strcmp(t, "create_circle")) {
        char *sr = strtok_r(NULL, " ", &save);
        char *sc = strtok_r(NULL, " ", &save);
        char *sf = strtok_r(NULL, " ", &save);
        if (!sr || !sc || !sf) { wstr(c->fd_w, "-1\n"); return; }
        long r2 = strtol(sr, NULL, 10);
        long fi = strtol(sf, NULL, 10);
        if (r2 < 0) { wstr(c->fd_w, "-2\n"); return; }
        struct sprite *sp = animate_create_circle((size_t)r2, (color_t)strtoul(sc, NULL, 0), (bool)fi);
        if (!sp) { wstr(c->fd_w, "-1\n"); return; }
        char r[64];
        snprintf(r, sizeof r, "0 %"PRIu64"\n", hadd(s, c, sp, HT_SPRITE));
        wstr(c->fd_w, r);
        return;
    }

    if (!strcmp(t, "create_sprite")) {
        char *fn = strtok_r(NULL, " ", &save);
        if (!fn) { wstr(c->fd_w, "-1\n"); return; }
        struct sprite *sp = animate_create_sprite(fn);
        if (!sp) { wstr(c->fd_w, "-1\n"); return; }
        char r[64];
        snprintf(r, sizeof r, "0 %"PRIu64"\n", hadd(s, c, sp, HT_SPRITE));
        wstr(c->fd_w, r);
        return;
    }

    if (!strcmp(t, "place_sprite")) {
        char *sc2 = strtok_r(NULL, " ", &save);
        char *ss  = strtok_r(NULL, " ", &save);
        char *sx  = strtok_r(NULL, " ", &save);
        char *sy  = strtok_r(NULL, " ", &save);
        if (!sc2 || !ss || !sx || !sy) { wstr(c->fd_w, "-1\n"); return; }
        struct canvas *cv = hlook(c, (uint64_t)strtoull(sc2, NULL, 10), HT_CANVAS);
        struct sprite *sp = hlook(c, (uint64_t)strtoull(ss,  NULL, 10), HT_SPRITE);
        if (!cv || !sp) { wstr(c->fd_w, "-1\n"); return; }
        struct sprite_placement *pl = animate_place_sprite(cv, sp,
            (ssize_t)strtol(sx, NULL, 10), (ssize_t)strtol(sy, NULL, 10));
        if (!pl) { wstr(c->fd_w, "-1\n"); return; }
        char r[64];
        snprintf(r, sizeof r, "0 %"PRIu64"\n", hadd(s, c, pl, HT_PLACEMENT));
        wstr(c->fd_w, r);
        return;
    }

    /* ── destroy_canvas: Q2 fix — only call animate_destroy_canvas when
     *    no other live client still holds a reference to this canvas.    */
    if (!strcmp(t, "destroy_canvas")) {
        char *si = strtok_r(NULL, " ", &save);
        if (!si) { wstr(c->fd_w, "-1\n"); return; }
        uint64_t id = (uint64_t)strtoull(si, NULL, 10);
        void *cv = hlook(c, id, HT_CANVAS);
        if (!cv) { wstr(c->fd_w, "-1\n"); return; }

        /* Remove this client's handle first so it is excluded from the
         * sharing check performed inside canvas_safe_destroy. */
        hrem(c, id);
        canvas_safe_destroy(s, id, cv);

        wstr(c->fd_w, "0\n");
        return;
    }

    if (!strcmp(t, "destroy_sprite") || !strcmp(t, "destroy_placement")) {
        char *si = strtok_r(NULL, " ", &save);
        if (!si) { wstr(c->fd_w, "-1\n"); return; }
        uint64_t id = (uint64_t)strtoull(si, NULL, 10);
        htype_t ht = !strcmp(t, "destroy_sprite") ? HT_SPRITE : HT_PLACEMENT;
        void *p = hlook(c, id, ht);
        if (!p) { wstr(c->fd_w, "-1\n"); return; }
        if (ht == HT_SPRITE) {
            animate_destroy_sprite(p);
            hrem(c, id);
            wstr(c->fd_w, "0 0\n");
        } else {
            animate_destroy_placement(p);
            hrem(c, id);
            wstr(c->fd_w, "0 0\n");
        }
        return;
    }

    if (!strcmp(t, "set_animation_params")) {
        char *si  = strtok_r(NULL, " ", &save);
        char *svx = strtok_r(NULL, " ", &save);
        char *svy = strtok_r(NULL, " ", &save);
        char *sax = strtok_r(NULL, " ", &save);
        char *say = strtok_r(NULL, " ", &save);
        if (!si || !svx || !svy || !sax || !say) { wstr(c->fd_w, "-1\n"); return; }
        struct sprite_placement *pl = hlook(c, (uint64_t)strtoull(si, NULL, 10), HT_PLACEMENT);
        if (!pl) { wstr(c->fd_w, "-1\n"); return; }
        animate_set_animation_params(pl,
            (ssize_t)strtol(svx, NULL, 10), (ssize_t)strtol(svy, NULL, 10),
            (ssize_t)strtol(sax, NULL, 10), (ssize_t)strtol(say, NULL, 10));
        wstr(c->fd_w, "0 0\n");
        return;
    }

    if (!strcmp(t, "placement_top")    || !strcmp(t, "placement_bottom") ||
        !strcmp(t, "placement_up")     || !strcmp(t, "placement_down")) {
        char *si = strtok_r(NULL, " ", &save);
        if (!si) { wstr(c->fd_w, "-1\n"); return; }
        struct sprite_placement *pl = hlook(c, (uint64_t)strtoull(si, NULL, 10), HT_PLACEMENT);
        if (!pl) { wstr(c->fd_w, "-1\n"); return; }
        if      (!strcmp(t, "placement_top"))    animate_placement_top(pl);
        else if (!strcmp(t, "placement_bottom")) animate_placement_bottom(pl);
        else if (!strcmp(t, "placement_up"))     animate_placement_up(pl);
        else                                     animate_placement_down(pl);
        wstr(c->fd_w, "0\n");
        return;
    }

    wstr(c->fd_w, "-1\n");
}

/* ── Worker thread (Q3: thread pool — any worker serves any client) ── */
static void *worker_fn(void *arg)
{
    srv_t  *s = arg;
    pool_t *p = &s->pool;
    pthread_mutex_lock(&p->mtx);
    while (1) {
        while (!p->head && !p->shutdown) pthread_cond_wait(&p->cv, &p->mtx);
        if (p->shutdown && !p->head) break;
        task_t *tk = p->head;
        p->head = tk->next;
        if (!p->head) p->tail = NULL;
        /* Release the queue lock before dispatching so other workers
         * can pick up tasks in parallel (true concurrent execution). */
        pthread_mutex_unlock(&p->mtx);
        if (tk->client->state != CS_DEAD) dispatch(s, tk->client, tk->cmd);
        free(tk);
        pthread_mutex_lock(&p->mtx);
    }
    pthread_mutex_unlock(&p->mtx);
    return NULL;
}

/* Q3: Enqueue a task onto the shared FIFO work queue and wake one worker. */
static int pool_enqueue(pool_t *p, client_t *c, const char *cmd)
{
    task_t *tk = malloc(sizeof *tk);
    if (!tk) return -1;
    tk->client = c;
    strncpy(tk->cmd, cmd, MAX_LINE - 1);
    tk->cmd[MAX_LINE - 1] = '\0';
    tk->next = NULL;
    pthread_mutex_lock(&p->mtx);
    if (p->tail) p->tail->next = tk;
    else p->head = tk;
    p->tail = tk;
    pthread_cond_signal(&p->cv);
    pthread_mutex_unlock(&p->mtx);
    return 0;
}

/* ── Connect client ── */
static void connect_client(srv_t *s, pid_t pid)
{
    pthread_mutex_lock(&s->cmtx);
    if (s->nclient >= MAX_CLIENTS) { pthread_mutex_unlock(&s->cmtx); return; }
    client_t *c = calloc(1, sizeof *c);
    if (!c) { pthread_mutex_unlock(&s->cmtx); return; }
    c->pid   = pid;
    c->state = CS_PENDING;
    c->fd_r  = -1; c->fd_w = -1;
    pthread_mutex_init(&c->hmtx, NULL);
    snprintf(c->path_c2s, sizeof c->path_c2s, "FIFO_C2S_%d", (int)pid);
    snprintf(c->path_s2c, sizeof c->path_s2c, "FIFO_S2C_%d", (int)pid);
    s->clients[s->nclient++] = c;
    pthread_mutex_unlock(&s->cmtx);
    unlink(c->path_c2s); unlink(c->path_s2c);
    if (mkfifo(c->path_c2s, 0600) != 0 || mkfifo(c->path_s2c, 0600) != 0) { c->state = CS_DEAD; return; }
    if (kill(pid, SIGUSR2) != 0) { c->state = CS_DEAD; return; }
    c->fd_r = open(c->path_c2s, O_RDONLY);
    c->fd_w = open(c->path_s2c, O_WRONLY);
    if (c->fd_r < 0 || c->fd_w < 0) { c->state = CS_DEAD; return; }
}

static void client_destroy(client_t *c)
{
    if (c->fd_w >= 0) close(c->fd_w);
    if (c->fd_r >= 0) close(c->fd_r);
    unlink(c->path_c2s); unlink(c->path_s2c);
    pthread_mutex_destroy(&c->hmtx);
    free(c);
}

int main(int argc, char **argv)
{
    if (argc < 2) return 1;
    int nthreads = atoi(argv[1]);
    int sigpipe[2];
    if (pipe(sigpipe) != 0) return 1;
    g_pipe_w = sigpipe[1];
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa_usr1 = {0};
    sa_usr1.sa_flags     = SA_SIGINFO | SA_RESTART;
    sa_usr1.sa_sigaction = on_sigusr1;
    sigaction(SIGUSR1, &sa_usr1, NULL);
    struct sigaction sa_term = {0};
    sa_term.sa_flags   = SA_RESTART;
    sa_term.sa_handler = on_sigterm;
    sigaction(SIGTERM, &sa_term, NULL);
    sigaction(SIGINT,  &sa_term, NULL);
    printf("Server PID: %d\n", (int)getpid());
    fflush(stdout);
    srv_t s;
    memset(&s, 0, sizeof s);
    s.alive      = 1;
    s.sigpipe[0] = sigpipe[0];
    s.sigpipe[1] = sigpipe[1];
    pthread_mutex_init(&s.cmtx,     NULL);
    pthread_mutex_init(&s.id_mtx,   NULL);
    pthread_mutex_init(&s.pool.mtx, NULL);
    pthread_cond_init (&s.pool.cv,  NULL);
    pthread_mutex_init(&s.bar_mtx,  NULL);
    pthread_cond_init (&s.bar_cv,   NULL);
    s.pool.workers  = calloc((size_t)nthreads, sizeof(pthread_t));
    s.pool.nworkers = nthreads;
    for (int i = 0; i < nthreads; i++)
        pthread_create(&s.pool.workers[i], NULL, worker_fn, &s);
    char linebuf[MAX_LINE];
    while (s.alive) {
        fd_set rfds; FD_ZERO(&rfds);
        FD_SET(s.sigpipe[0], &rfds);
        int maxfd = s.sigpipe[0];
        pthread_mutex_lock(&s.cmtx);
        for (int i = 0; i < s.nclient; i++) {
            if (s.clients[i]->state != CS_DEAD && s.clients[i]->fd_r >= 0) {
                FD_SET(s.clients[i]->fd_r, &rfds);
                if (s.clients[i]->fd_r > maxfd) maxfd = s.clients[i]->fd_r;
            }
        }
        pthread_mutex_unlock(&s.cmtx);
        struct timeval tv = { 2, 0 };
        if (select(maxfd + 1, &rfds, NULL, NULL, &tv) < 0) { if (errno == EINTR) continue; break; }
        if (FD_ISSET(s.sigpipe[0], &rfds)) {
            pid_t pid;
            if (read(s.sigpipe[0], &pid, sizeof pid) == sizeof pid) {
                if      (pid == 0) s.alive = 0;
                else if (pid > 0)  connect_client(&s, pid);
            }
        }
        pthread_mutex_lock(&s.cmtx);
        for (int i = 0; i < s.nclient; i++) {
            client_t *c = s.clients[i];
            if (c->state == CS_DEAD || c->fd_r < 0 || !FD_ISSET(c->fd_r, &rfds)) continue;
            pthread_mutex_unlock(&s.cmtx);
            ssize_t n = read(c->fd_r, linebuf, sizeof linebuf - 1);
            if (n > 0) {
                linebuf[n] = '\0';
                if (!strncmp(linebuf, "Login ", 6)) dispatch(&s, c, linebuf);
                else pool_enqueue(&s.pool, c, linebuf);
            } else c->state = CS_DEAD;
            pthread_mutex_lock(&s.cmtx);
        }

        /* ── Dead client cleanup (Q2: release canvas handles safely) ── */
        for (int i = 0; i < s.nclient; i++) {
            client_t *c = s.clients[i];
            /* Detect ungraceful exit */
            if (c->state != CS_DEAD && kill(c->pid, 0) != 0 && errno == ESRCH)
                c->state = CS_DEAD;

            if (c->state == CS_DEAD) {
                /* Drop FDs and FIFOs first so no new commands can arrive */
                if (c->fd_w >= 0) { close(c->fd_w); c->fd_w = -1; }
                if (c->fd_r >= 0) { close(c->fd_r); c->fd_r = -1; }
                unlink(c->path_c2s); unlink(c->path_s2c);

                /* Q2: release canvas resources, destroying only those
                 * not shared with any remaining live client.           */
                pthread_mutex_unlock(&s.cmtx);
                client_release_canvases(&s, c);
                pthread_mutex_lock(&s.cmtx);
            }
        }
        pthread_mutex_unlock(&s.cmtx);
    }

    /* ── Shutdown: drain thread pool then free all clients ── */
    pthread_mutex_lock(&s.pool.mtx);
    s.pool.shutdown = 1;
    pthread_cond_broadcast(&s.pool.cv);
    pthread_mutex_unlock(&s.pool.mtx);
    for (int i = 0; i < nthreads; i++) pthread_join(s.pool.workers[i], NULL);
    free(s.pool.workers);

    for (int i = 0; i < s.nclient; i++) client_destroy(s.clients[i]);
    return 0;
}