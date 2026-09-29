#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_LINE 1024

static volatile sig_atomic_t got_usr2 = 0;
static void on_sigusr2(int s) { (void)s; got_usr2 = 1; }

int main(int argc, char **argv)
{
    /* ── Argument validation ─────────────────────────────────────── */
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <server_pid>\n", argv[0]);
        return 1;
    }

    errno = 0;
    char *endp;
    long raw_pid = strtol(argv[1], &endp, 10);
    if (errno != 0 || endp == argv[1] || *endp != '\0' || raw_pid <= 0) {
        fprintf(stderr, "Error: invalid server PID '%s'\n", argv[1]);
        return 1;
    }
    pid_t server_pid = (pid_t)raw_pid;
    pid_t my_pid     = getpid();

    /* Make stdout line-buffered so piped tests receive output promptly. */
    setvbuf(stdout, NULL, _IOLBF, 0);

    /* ── Set up SIGUSR2 handler before sending SIGUSR1 ───────────── */
    struct sigaction sa = {0};
    sa.sa_handler = on_sigusr2;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGUSR2, &sa, NULL) != 0) {
        perror("sigaction SIGUSR2");
        return 1;
    }

    /* ── Send SIGUSR1 to server (IPC: signal to server process) ─── */
    if (kill(server_pid, SIGUSR1) != 0) {
        perror("kill SIGUSR1");
        return 1;
    }

    /* ── Wait for SIGUSR2 back (~1 second timeout, 100 ms steps) ── */
    int waited = 0;
    while (!got_usr2 && waited < 10) {
        usleep(100000);
        waited++;
    }
    if (!got_usr2) {
        fprintf(stderr, "Timeout: no response from server\n");
        return 1;
    }

    /* ── Build FIFO paths (IPC: named pipes) ─────────────────────── */
    char path_c2s[64], path_s2c[64];
    snprintf(path_c2s, sizeof path_c2s, "FIFO_C2S_%d", (int)my_pid);
    snprintf(path_s2c, sizeof path_s2c, "FIFO_S2C_%d", (int)my_pid);

    /* ── Open FIFOs ──────────────────────────────────────────────── */
    int fd_w = open(path_c2s, O_WRONLY);
    if (fd_w < 0) {
        fprintf(stderr, "open %s: %s\n", path_c2s, strerror(errno));
        return 1;
    }

    int fd_r = open(path_s2c, O_RDONLY);
    if (fd_r < 0) {
        fprintf(stderr, "open %s: %s\n", path_s2c, strerror(errno));
        close(fd_w);
        return 1;
    }

    /* ── Main loop: stdin → server → stdout ──────────────────────── */
    char line[MAX_LINE];
    char resp[MAX_LINE];
    int  exit_code = 0;

    while (fgets(line, sizeof line, stdin)) {

        /* Strip trailing CR / LF */
        line[strcspn(line, "\r\n")] = '\0';

        /* Skip blank lines */
        if (line[0] == '\0') continue;

        /* Build the newline-terminated message for the server */
        char msg[MAX_LINE];
        int  msg_len = snprintf(msg, sizeof msg, "%s\n", line);
        if (msg_len < 0 || msg_len >= (int)sizeof msg) {
            fprintf(stderr, "Command too long, skipping\n");
            continue;
        }

        /* ── Send command to server (Device I/O: write to FIFO) ─── */
        ssize_t written = write(fd_w, msg, (size_t)msg_len);
        if (written < 0) {
            fprintf(stderr, "write to server: %s\n", strerror(errno));
            exit_code = 1;
            break;
        }
        if (written != (ssize_t)msg_len) {
            fprintf(stderr, "Partial write to server\n");
            exit_code = 1;
            break;
        }

        /* ── After Disconnect: no ACK expected — exit cleanly ────── */
        if (strcmp(line, "Disconnect") == 0) break;

        /* ── Read response from server ───────────────────────────── */
        ssize_t n = read(fd_r, resp, sizeof resp - 1);
        if (n < 0) {
            fprintf(stderr, "read from server: %s\n", strerror(errno));
            exit_code = 1;
            break;
        }
        if (n == 0) {
            fprintf(stderr, "Server disconnected\n");
            break;
        }
        resp[n] = '\0';
        resp[strcspn(resp, "\r\n")] = '\0';

        /* ── Translate server response for user (string manip) ───── */
        if (strncmp(resp, "Reject ", 7) == 0) {
            /* Authentication rejection — print as-is then exit */
            printf("%s\n", resp);
            break;

        } else if (resp[0] == '-' && resp[1] == '1') {
            printf("RPC Failed\n");

        } else if (resp[0] == '-' && resp[1] == '2') {
            printf("Value error\n");

        } else if (strncmp(resp, "0 -1", 4) == 0) {
            printf("Data write failed\n");

        } else if (strncmp(resp, "0 0 -1", 6) == 0) {
            printf("Movie write failed\n");

        } else if (strncmp(resp, "0 0 0", 5) == 0) {
            printf("Success\n");

        } else if (strncmp(resp, "0 ", 2) == 0) {
            /* Success with a return value (e.g. handle ID) */
            printf("Success %s\n", resp + 2);

        } else if (strcmp(resp, "0") == 0) {
            printf("Success\n");

        } else {
            /*
             * Login response: a bare integer (balance) or a plain
             * text message forwarded from the server.
             */
            char *ep;
            long bal = strtol(resp, &ep, 10);
            if (ep != resp && *ep == '\0') {
                /* Extract username: line is "Login <username>" */
                const char *u = line + 6;
                printf("Welcome %s. Your balance is %ld\n", u, bal);
            } else {
                printf("%s\n", resp);
            }
        }
    }

    /* ── Cleanup ─────────────────────────────────────────────────── */
    if (close(fd_w) != 0)
        fprintf(stderr, "close fd_w: %s\n", strerror(errno));
    if (close(fd_r) != 0)
        fprintf(stderr, "close fd_r: %s\n", strerror(errno));

    return exit_code;
}