#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "./comm.h"
#include "./db.h"

/*
 * Use the variables in this struct to synchronize your main thread with client
 * threads. Note that all client threads must have terminated before you clean
 * up the database.
 */
typedef struct server_control {
    pthread_mutex_t server_mutex;
    pthread_cond_t server_cond;
    int num_client_threads;
} server_control_t;

/*
 * Controls when the clients in the client thread list should be stopped and
 * let go.
 */
typedef struct client_control {
    pthread_mutex_t go_mutex;
    pthread_cond_t go;
    int stopped;
} client_control_t;

/*
 * The encapsulation of a client thread, i.e., the thread that handles
 * commands from clients.
 */
typedef struct client {
    pthread_t thread;
    FILE *cxstr;  // File stream for input and output

    // For client list
    struct client *prev;
    struct client *next;
} client_t;

/*
 * The encapsulation of a thread that handles signals sent to the server.
 * When SIGINT is sent to the server all client threads should be destroyed.
 */
typedef struct sig_handler {
    sigset_t set;
    pthread_t thread;
} sig_handler_t;

typedef enum {false, true} bool;

client_t *thread_list_head;
pthread_mutex_t thread_list_mutex = PTHREAD_MUTEX_INITIALIZER;
client_control_t client_control = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,
                           0};
server_control_t server_control = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,
                           0};
bool client_accept_flag = true;

void *run_client(void *arg);
void *monitor_signal(void *arg);
void thread_cleanup(void *arg);

// Called by client threads to wait until progress is permitted
void client_control_wait() {
    // TODO: Block the calling thread until the main thread calls
    // client_control_release(). See the client_control_t struct.
    pthread_mutex_lock(&client_control.go_mutex);
    pthread_cleanup_push((void (*)(void *))pthread_mutex_unlock, (void *)&client_control.go_mutex);
    while ((int)client_control.stopped) {
        pthread_cond_wait(&client_control.go, &client_control.go_mutex);
    }
    pthread_cleanup_pop(1);
}

// Called by main thread to stop client threads
void client_control_stop() {
    // TODO: Ensure that the next time client threads call client_control_wait()
    // at the top of the event loop in run_client, they will block.
    pthread_mutex_lock(&client_control.go_mutex);
    client_control.stopped = 1;
    pthread_mutex_unlock(&client_control.go_mutex);
}

// Called by main thread to resume client threads
void client_control_release() {
    // TODO: Allow clients that are blocked within client_control_wait()
    // to continue. See the client_control_t struct.
    pthread_mutex_lock(&client_control.go_mutex);
    client_control.stopped = 0;
    pthread_cond_broadcast(&client_control.go);
    pthread_mutex_unlock(&client_control.go_mutex);
}

// Called by listener (in comm.c) to create a new client thread
void client_constructor(FILE *cxstr) {
    // You should create a new client_t struct here and initialize ALL
    // of its fields. Remember that these initializations should be
    // error-checked.
    //
    // TODO:
    // Step 1: Allocate memory for a new client and set its connection stream
    // to the input argument.
    client_t *client = (client_t *)malloc(sizeof(client_t));
    client->cxstr = cxstr;
    // Step 2: Create the new client thread running the run_client routine.
    int c = pthread_create(&client->thread, 0, (void *(*)(void *))run_client, (void *)client);
    if (c) {
        errno = c;
        perror("create");
        exit(EXIT_FAILURE);
    }
    // Step 3: Detach the new client thread
    int d = pthread_detach(client->thread);
    if (d) {
        errno = d;
        perror("detach");
        exit(EXIT_FAILURE);
    }
}

void client_destructor(client_t *client) {
    // TODO: Free and close all resources associated with a client.
    // Whatever was malloc'd in client_constructor should
    // be freed here!
    comm_shutdown(client->cxstr);
    free(client);
}

// Code executed by a client thread
void *run_client(void *arg) {
    // TODO:
    // Step 1: Make sure that the server is still accepting clients. This will
    //         will make sense when handling EOF for the server.
    if (client_accept_flag == false) {
        return (void *)1;
    }
    // Step 2: Add client to the client list and push thread_cleanup to remove
    //       it if the thread is canceled.
    client_t *client = (client_t *)arg;

    if (thread_list_head == NULL) {
        thread_list_head = client;
    } else {
        client->next = thread_list_head;
        client->prev = NULL;
        thread_list_head->prev = client;
        thread_list_head= client;
    }
    pthread_mutex_unlock(&thread_list_mutex);
    pthread_mutex_lock(&server_control.server_mutex);
    server_control.num_client_threads++;
    pthread_mutex_unlock(&server_control.server_mutex);
    pthread_cleanup_push(thread_cleanup, client);

    char res[BUFLEN];
    memset(res, 0 ,BUFLEN);

    char comm[BUFLEN];
    memset(comm, 0, BUFLEN);
    // Step 3: Loop comm_serve (in comm.c) to receive commands and output
    //       responses. Execute commands using interpret_command (in db.c)
    while (comm_serve(client->cxstr, res, comm) != -1) {
        client_control_wait();
        interpret_command(comm, res, BUFLEN);
    }
    
    // Step 4: When the client is done sending commands, exit the thread
    //       cleanly.
    pthread_cleanup_pop(1);
    // You will need to modify this when implementing functionality for stop and
    // go!
    return NULL;
}

void delete_all() {
    // TODO: Cancel every thread in the client thread list with the
    // pthread_cancel function.
    pthread_mutex_lock(&server_control.server_mutex);
    client_t *curr_thread = thread_list_head;
    client_t *next_thread;

    while (curr_thread != NULL) {
        next_thread = curr_thread->next;
        pthread_cancel(curr_thread->thread);
        curr_thread = next_thread;

    }
    pthread_mutex_unlock(&server_control.server_mutex);
}

// Cleanup routine for client threads, called on cancels and exit.
void thread_cleanup(void *arg) {
    // TODO: Remove the client object from thread list and call
    // client_destructor. This function must be thread safe! The client must
    // be in the list before this routine is ever run.
    client_t *client = (client_t *)arg;
    pthread_mutex_lock(&thread_list_mutex);
    if (client->next == NULL) { 
        // only client
        if (client->prev == NULL) { 
            thread_list_head = NULL;
        // last client
        } else { 
            client->prev->next = NULL;
        }
    // first client
    }  else if (client == thread_list_head) { 
        thread_list_head = client->next;
    } else {
        client->prev->next = client->next;
        client->next->prev = client->prev;
    }

    pthread_mutex_unlock(&thread_list_mutex);
    client_destructor(client);
    pthread_mutex_lock(&server_control.server_mutex);
    server_control.num_client_threads = server_control.num_client_threads - 1;
    if(server_control.num_client_threads == 0) {
        pthread_cond_broadcast(&server_control.server_cond);
    }
    pthread_mutex_unlock(&server_control.server_mutex);
}

// Code executed by the signal handler thread. For the purpose of this
// assignment, there are two reasonable ways to implement this.
// The one you choose will depend on logic in sig_handler_constructor.
// 'man 7 signal' and 'man sigwait' are both helpful for making this
// decision. One way or another, all of the server's client threads
// should terminate on SIGINT. The server (this includes the listener
// thread) should not, however, terminate on SIGINT!
void *monitor_signal(void *arg) {
    // TODO: Wait for a SIGINT to be sent to the server process and cancel
    // all client threads when one arrives.
    int signal;
    sigset_t *sigset = (sigset_t *)arg;
    int s;

    while (1) {
        s = sigwait(sigset, &signal);
        if (s) {
            errno = s;
            perror("sigwait");
            exit(EXIT_FAILURE);
        }
        if (signal == SIGINT) {
            s = fprintf(stdout, "SIGINT\n");
            if (s < 0) {
                errno = s;
                perror("fprintf");
                exit(EXIT_FAILURE);
            }
            fflush(stdout);
            delete_all();
        }
    }
    return (void *)1;
}

sig_handler_t *sig_handler_constructor() {
    // TODO: Create a thread to handle SIGINT. The thread that this function
    // creates should be the ONLY thread that ever responds to SIGINT.
    sig_handler_t *sig_handler = (sig_handler_t *)malloc(sizeof(sig_handler_t));
    sigemptyset(&sig_handler->set);
    sigaddset(&sig_handler->set, SIGINT);
    int s = pthread_sigmask(SIG_BLOCK, &sig_handler->set, NULL);
    if(s) {
        free(sig_handler);
        errno = s;
        perror("pthread_sigmask");
        exit(EXIT_FAILURE);
    }
    int c = pthread_create(&sig_handler->thread, 0, (void *(*)(void *))monitor_signal, &sig_handler->set);
    if(c) {
        free(sig_handler);
        errno = c;
        perror("pthread_create");
        exit(EXIT_FAILURE);
    }
    return sig_handler;
}

void sig_handler_destructor(sig_handler_t *sighandler) {
    // TODO: Free any resources allocated in sig_handler_constructor.
    // Cancel and join with the signal handler's thread.
    int c = pthread_cancel(sighandler->thread);
    if (c) {
        errno = c;
        perror("pthread_cancel");
        exit(EXIT_FAILURE);
    }
    int j = pthread_join(sighandler->thread, NULL);
    if (j) {
        errno = j;
        perror("pthread_cancel");
        exit(EXIT_FAILURE);
    }
    free(sighandler);
}

// The arguments to the server should be the port number.
int main(int argc, char *argv[]) {
    // TODO:
    if (argc != 2) {
        fprintf(stderr, "Not right amount (2) of arguments.");
        exit(EXIT_FAILURE);
    }

    int port_num = atoi(argv[1]);
    if (port_num < 1024) {
        fprintf(stderr, "Port number less than 1024");
        exit(EXIT_FAILURE);
    }

    // Step 1: Set up the signal handler for handling SIGINT.
    sig_handler_t *sigint_handler = sig_handler_constructor();

    // Step 2: ignore SIGPIPE so that the server does not abort when a client
    // disconnects 
    sigset_t sigset;
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGPIPE);

    int s = pthread_sigmask(SIG_BLOCK, &sigset, NULL);
    if (s) {
        errno = s;
        perror("pthread_sigmask");
        exit(EXIT_FAILURE);
    }

    // Step 3: Start a listener thread for clients (see
    // start_listener in
    //       comm.c).
    
    pthread_t thread_listener;
    thread_listener = start_listener(port_num, &client_constructor);
    int r;
    char comm[BUFLEN];
    memset(comm, 0, BUFLEN);
    
    // Step 4: Loop for command line input and handle accordingly until EOF.
    while(1) {
        r = read(STDIN_FILENO, comm, BUFLEN);
        if (r == -1) {
            perror("read");
            exit(EXIT_FAILURE);
        }

        if (!r) {
            client_accept_flag = false; 
            break;
        }

        if (comm[0] == 'g') {
            client_control_release();
        } else if (comm[0] == 's') {
            client_control_stop();
        } else if (comm[0] == 'p') {
            char *f = strtok(&comm[1], "\t\r\n");
            int p = db_print(f);
            if (p) {
                errno = p;
                perror("db_print");
                exit(EXIT_FAILURE);
            }
        }
        memset(comm, 0, BUFLEN);
    }
    // Step 5: Destroy the signal handler, delete all clients, cleanup the
    //       database, cancel and join with the listener thread
    //
    sig_handler_destructor(sigint_handler);
    // You should ensure that the thread list is empty before cleaning up the
    // database and canceling the listener thread. Think carefully about what
    // happens in a call to delete_all() and ensure that there is no way for a
    // thread to add itself to the thread list after the server's final
    // delete_all().
    int c = pthread_cancel(thread_listener);
    if (c) {
        errno = c;
        perror("pthread_cancel");
        exit(EXIT_FAILURE);
    }
    int j = pthread_join(thread_listener, NULL);
    if (j) {
        errno = j;
        perror("pthread_join");
        exit(EXIT_FAILURE);
    }
    delete_all();
    pthread_mutex_lock(&server_control.server_mutex);
    while (server_control.num_client_threads) {
        pthread_cond_wait(&server_control.server_cond, &server_control.server_mutex);
    }
    pthread_mutex_unlock(&server_control.server_mutex);
    db_cleanup();
    pthread_exit((void *)0);

    return 0;
}
