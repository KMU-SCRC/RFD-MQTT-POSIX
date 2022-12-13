/*
 * RFD MQTT
 */

#include <mosquitto.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <signal.h>
#ifdef _WIN32
#include <Windows.h>
#include <direct.h>
#include "getopt.c"
#define fileno _fileno
#pragma warning(disable: 4996)
#else
#include <sys/time.h>
#include <unistd.h>
#endif


static unsigned int quit = 0;
static unsigned char id[50] = "AA";
static unsigned char topic[50] = "PENGUIN/1";
static unsigned char filetopic[60];
static unsigned char filename[100] = "2MB_PENGUIN.csv";
static unsigned char filename_origin[100] = "2MB_PENGUIN.csv";
static unsigned char* filename_cut;
#ifdef _WIN32
static unsigned char fileroot[200] = "D:\\data\\sampledata.csv";
static unsigned char logroot[200] = "D:\\data\\RFD_MQTT_SYSTEM_LOG.TXT";
static unsigned char statroot[200] = "D:\\data\\RFD_NETWORK_STAT.TXT";
static unsigned char configroot[200] = "D:\\RFDCONFIG\\RFDMQTTCONFIG.INI";
#else
static unsigned char fileroot[200] = "/home/pi/Projects/data/sampledata.csv";
static unsigned char logroot[200] = "/home/pi/Projects/data/RFD_MQTT_SYSTEM_LOG.TXT";
static unsigned char statroot[200] = "/home/pi/Projects/data/RFD_NETWORK_STAT.TXT";
static unsigned char configroot[200] = "/home/pi/Projects/RFDCONFIG/RFDMQTTCONFIG.INI";
#endif
static FILE* logfile = NULL;
static unsigned char urihost[100] = "kmuscrc-ffd";
//static unsigned char urihost_input[80] = "kmuscrc-ffd";

static unsigned int start_switch = 0;
static unsigned int stop_switch = 0;
static unsigned int restart_switch = 0;
static unsigned int retry_switch = 0;
static unsigned int re_switch = 0;
//static unsigned int done_switch = 0;
//static unsigned int continue_switch = 0;
static unsigned int delete_switch = 0;
static unsigned int exit_switch = 0;
static unsigned int stat_switch = 0;
static unsigned int infinity_switch = 0;
static unsigned int infinity_count = 1;
static unsigned int timer = 0;
static unsigned int restart_count = 0;
static unsigned int count = 1000;
//double dif;
static time_t ptime, ftime;
#ifdef _WIN32
static clock_t startc, endc;
#else
static struct timeval startc = {};
static struct timeval endc = {};
#endif
static struct tm* pinfo;
static struct tm* finfo;

static FILE* file = NULL;
static unsigned char fileBuffer[1000];
static unsigned int number = 1;
static unsigned int toggle = 0;
static unsigned int maxnum = 1;
static unsigned int difnum = 0;
static unsigned int pernum = 1;
//static unsigned char url[200] = "MQTT://kmuscrc-ffd/file?";
unsigned char payload[1000];
unsigned char logbuf[1000];


static void MakeDirectory(unsigned char* full_path)
{
    unsigned char temp[256], * sp;
    strcpy(temp, full_path); // 경로문자열을 복사
    sp = temp; // 포인터를 문자열 처음으로

    while ((sp = strchr(sp, '\\'))) { // 디렉토리 구분자를 찾았으면
        if (sp > temp && *(sp - 1) != ':') { // 루트디렉토리가 아니면
            *sp = '\0'; // 잠시 문자열 끝으로 설정
            #ifdef _WIN32
            _mkdir(temp);
            #else
            mkdir(temp, 0777);
            #endif
            // 디렉토리를 만들고 (존재하지 않을 때)
            * sp = '\\'; // 문자열을 원래대로 복귀
        }
        sp++; // 포인터를 다음 문자로 이동
    }

}


static void
handle_sigint(int signum) {
    quit = 1;
}


 /* Callback called when the client receives a CONNACK message from the broker. */
static void on_connect(struct mosquitto* mosq, void* obj, int reason_code)
{
	int rc;
	/* Print out the connection result. mosquitto_connack_string() produces an
	 * appropriate string for MQTT v3.x clients, the equivalent for MQTT v5.0
	 * clients is mosquitto_reason_string().
	 */
	printf("on_connect: %s\n", mosquitto_connack_string(reason_code));
	//if (reason_code != 0) {
		/* If the connection fails for any reason, we don't want to keep on
		 * retrying in this example, so disconnect. Without this, the client
		 * will attempt to reconnect. */
		//mosquitto_disconnect(mosq);
	//}

	/* Making subscriptions in the on_connect() callback means that if the
	 * connection drops and is automatically resumed by the client, then the
	 * subscriptions will be recreated when the client reconnects. */
	rc = mosquitto_subscribe(mosq, NULL, "ALL", 0);
	rc = mosquitto_subscribe(mosq, NULL, id, 0);
	//if (rc != MOSQ_ERR_SUCCESS) {
		//fprintf(stderr, "Error subscribing: %s\n", mosquitto_strerror(rc));
		/* We might as well disconnect if we were unable to subscribe */
		//mosquitto_disconnect(mosq);
	//}
}


/* Callback called when the broker sends a SUBACK in response to a SUBSCRIBE. */
static void on_subscribe(struct mosquitto* mosq, void* obj, int mid, int qos_count, const int* granted_qos)
{
	int i;
	bool have_subscription = false;

	/* In this example we only subscribe to a single topic at once, but a
	 * SUBSCRIBE can contain many topics at once, so this is one way to check
	 * them all. */
	for (i = 0; i < qos_count; i++) {
		printf("on_subscribe: %d:granted qos = %d\n", i, granted_qos[i]);
		if (granted_qos[i] <= 2) {
			have_subscription = true;
		}
	}
	if (have_subscription == false) {
		/* The broker rejected all of our subscriptions, we know we only sent
		 * the one SUBSCRIBE, so there is no point remaining connected. */
		fprintf(stderr, "Error: All subscriptions rejected.\n");
		mosquitto_disconnect(mosq);
	}
}


/* Callback called when the client knows to the best of its abilities that a
 * PUBLISH has been successfully sent. For QoS 0 this means the message has
 * been completely written to the operating system. For QoS 1 this means we
 * have received a PUBACK from the broker. For QoS 2 this means we have
 * received a PUBCOMP from the broker. */
static void on_publish(struct mosquitto* mosq, void* obj, int mid)
{
	//printf("Message with mid %d has been published.\n", mid);
}


/* Callback called when the client receives a message. */
static void on_message(struct mosquitto* mosq, void* obj, const struct mosquitto_message* msg)
{
	/* This blindly prints the payload, but the payload can be anything so take care. */
	//printf("%s %d %s\n", msg->topic, msg->qos, (char*)msg->payload);
	char str[200], * cut;

    cut = strtok((char*)msg->payload, ":");
    if (strcmp(cut, "OK") == 0) {
        count = 1000;
        if (!start_switch) {
            toggle = atoi(strtok(NULL, ":"));
            start_switch = 1;
        }
        else { return; }
    }
    else if (strcmp(cut, "STOP") == 0) {
        count = 1000;
        if (stop_switch) { return; }
        stop_switch = 1;
    }
    else if (strcmp(cut, "RESTART") == 0) {
        if (re_switch) {
            count = 1000;
            if (restart_switch || retry_switch) { return; }
            toggle = atoi(strtok(NULL, ":"));
            if (toggle == 0) {
                retry_switch = 1;
                restart_switch = 0;
            }
            else {
                retry_switch = 0;
                restart_switch = 1;
            }
            re_switch = 0;
        }
    }
    else if (strcmp(cut, "DELETE") == 0) {
        count = 1000;
        if (delete_switch) { return; }
		#ifdef _WIN32
        endc = clock();
		#else
        gettimeofday(&endc, NULL);
		#endif
        time(&ftime);
        finfo = localtime(&ftime);
        delete_switch = 1;
        logfile = fopen(logroot, "a");
        cut = strtok(NULL, ":");
        if (strcmp(cut, "E") == 0) {
            sprintf(str, "서버측에서 기존에 파일을 이미 다 받음\n");
            printf(str);
            fputs(str, logfile);
        }
        sprintf(str, "END : %s", asctime(finfo));
        printf(str);
        fputs(str, logfile);
        if (timer) {
			#ifdef _WIN32
            sprintf(str, "TOTAL : %lf, restart : %d\n", (double)(endc - startc) / CLOCKS_PER_SEC, restart_count);
			#else
            sprintf(str, "TOTAL : %lf, restart : %d\n", (double)(endc.tv_sec + endc.tv_usec / 1000000.0 - startc.tv_sec - startc.tv_usec / 1000000.0), restart_count);
			#endif
        }
        else {
            sprintf(str, "TOTAL : 0.0, restart : 0\n");
        }
        printf(str);
        fputs(str, logfile);
        fflush(logfile);
        fclose(logfile);
    }
    else if (strcmp(cut, "ERR") == 0) {
        count = 1000;
        quit = 1;
        cut = strtok(NULL, ":");
        if (strcmp(cut, "M") == 0) {
            printf("서버의 구조체 생성 오류\n");
        }
        else if (strcmp(cut, "F") == 0) {
            printf("기존과 다른 파일을 서버가 가지고 있음(전송전)\n");
        }
        else if (strcmp(cut, "P") == 0) {
            printf("기존과 다른 파일을 서버가 가지고 있음(전송중)\n");
        }
        else if (strcmp(cut, "D") == 0) {
            printf("기존과 다른 파일을 서버가 가지고 있음(전송후)\n");
        }
        else if (strcmp(cut, "N") == 0) {
            printf("쿼리 없이 명령어 전송\n");
        }
        else if (strcmp(cut, "W") == 0) {
            printf("잘못된 명령어 전송\n");
        }
        else if (strcmp(cut, "R") == 0) {
            printf("잘못된 파일 전송\n");
        }
        else {
            printf("오류\n");
        }
    }
    else if (strcmp(cut, "ACK") == 0) {
        quit = 1;
    }
}


int main(int argc, char** argv)
{
	struct mosquitto* mosq;
	int rc;

    int opt;
    unsigned char* ID_CUT;
    unsigned char* TOPIC_CUT;
    unsigned char* FILENAME_CUT;
    unsigned char* FILEROOT_CUT;
    unsigned char* LOGROOT_CUT;
    unsigned char* URIHOST_CUT;
    unsigned char* QUALITY_CUT;
    unsigned char* SIGNAL_LEVEL_CUT;
    struct stat statbuf;
    size_t len;
    unsigned int rewrite_switch = 0;
    unsigned int exit_count = 10;
    unsigned int start_count = 10;
    unsigned int ok_count = 10;
    unsigned int done_count = 10;
    unsigned int empty_count = 10;

    #ifndef _WIN32
    struct sigaction sa;
    #endif

    MakeDirectory(configroot);

    file = fopen(configroot, "r");
    if (file == NULL) {
        rewrite_switch = 1;
    }
    else {
        if (fstat(fileno(file), &statbuf) < 0) {
            rewrite_switch = 1;
        }
        else {
            len = fread(fileBuffer, 1, statbuf.st_size, file);
            if (len < 0) {
                rewrite_switch = 1;
            }
            else {
                ID_CUT = strtok(fileBuffer, "\n");
                TOPIC_CUT = strtok(NULL, "\n");
                FILENAME_CUT = strtok(NULL, "\n");
                FILEROOT_CUT = strtok(NULL, "\n");
                LOGROOT_CUT = strtok(NULL, "\n");
                URIHOST_CUT = strtok(NULL, "\n");
                if ((ID_CUT != NULL) && (TOPIC_CUT != NULL) && (FILENAME_CUT != NULL) && (FILEROOT_CUT != NULL) && (LOGROOT_CUT != NULL) && (URIHOST_CUT != NULL) && ((ID_CUT = strtok(ID_CUT, "=")) != NULL) && (strcmp(ID_CUT, "ID") == 0) && ((ID_CUT = strtok(NULL, "=")) != NULL) && ((TOPIC_CUT = strtok(TOPIC_CUT, "=")) != NULL) && (strcmp(TOPIC_CUT, "TOPIC") == 0) && ((TOPIC_CUT = strtok(NULL, "=")) != NULL) && ((FILENAME_CUT = strtok(FILENAME_CUT, "=")) != NULL) && (strcmp(FILENAME_CUT, "FILENAME") == 0) && ((FILENAME_CUT = strtok(NULL, "=")) != NULL) && ((FILEROOT_CUT = strtok(FILEROOT_CUT, "=")) != NULL) && (strcmp(FILEROOT_CUT, "FILEROOT") == 0) && ((FILEROOT_CUT = strtok(NULL, "=")) != NULL) && ((LOGROOT_CUT = strtok(LOGROOT_CUT, "=")) != NULL) && (strcmp(LOGROOT_CUT, "LOGROOT") == 0) && ((LOGROOT_CUT = strtok(NULL, "=")) != NULL) && ((URIHOST_CUT = strtok(URIHOST_CUT, "=")) != NULL) && (strcmp(URIHOST_CUT, "URIHOST") == 0) && ((URIHOST_CUT = strtok(NULL, "=")) != NULL)) {
                    sprintf(id, ID_CUT);
                    sprintf(topic, TOPIC_CUT);
                    sprintf(filename, FILENAME_CUT);
                    sprintf(fileroot, FILEROOT_CUT);
                    sprintf(logroot, LOGROOT_CUT);
                    sprintf(urihost, URIHOST_CUT);
                }
                else {
                    rewrite_switch = 1;
                }
            }
        }
        fclose(file);
    }

    if (rewrite_switch) {
        file = fopen(configroot, "w");
        #ifdef _WIN32
        sprintf(fileBuffer, "ID=AA\nTOPIC=PENGUIN/1\nFILENAME=2MB_PENGUIN.csv\nFILEROOT=D:\\data\\sampledata.csv\nLOGROOT=D:\\data\\RFD_MQTT_SYSTEM_LOG.TXT\nURIHOST=kmuscrc-ffd\n\n\n\n ID(RFD 기기의 고유값)\n TOPIC(RFD가 보내고자 하는 파일의 주제)\n FILENAME(FFD에 저장될 파일명)\n FILEROOT(RFD가 보내고자 하는 파일 전체 경로)\n LOGROOT(RFD가 로그 파일을 저장할 전체 경로)\n URIHOST(FFD의 HOST주소)\n 순으로 변수명뒤에 띄어쓰기 없이 =과 텍스트를 붙임으로서 설정할 수 있으며 줄바꿈으로 값을 구별합니다.\n");
        #else
        sprintf(fileBuffer, "ID=AA\nTOPIC=PENGUIN/1\nFILENAME=2MB_PENGUIN.csv\nFILEROOT=/home/pi/Projects/data/sampledata.csv\nLOGROOT=/home/pi/Projects/data/RFD_MQTT_SYSTEM_LOG.TXT\nURIHOST=kmuscrc-ffd\n\n\n\n ID(RFD 기기의 고유값)\n TOPIC(RFD가 보내고자 하는 파일의 주제)\n FILENAME(FFD에 저장될 파일명)\n FILEROOT(RFD가 보내고자 하는 파일 전체 경로)\n LOGROOT(RFD가 로그 파일을 저장할 전체 경로)\n URIHOST(FFD의 HOST주소)\n 순으로 변수명뒤에 띄어쓰기 없이 =과 텍스트를 붙임으로서 설정할 수 있으며 줄바꿈으로 값을 구별합니다.\n");
        #endif
        fputs(fileBuffer, file);
        fflush(file);
        fclose(file);
    }
    rewrite_switch = 0;

    while ((opt = getopt(argc, argv, "a:D:Ef:F:I:tT:U:")) != -1) {
        switch (opt) {
        case 'a':
            sprintf(statroot, optarg);
            stat_switch = 1;
            break;
        case 'D':
            sprintf(logroot, optarg);
            rewrite_switch = 1;
            break;
        case 'E':
            exit_switch = 1;
            break;
        case 'f':
            sprintf(filename, optarg);
            rewrite_switch = 1;
            break;
        case 'F':
            sprintf(fileroot, optarg);
            rewrite_switch = 1;
            break;
        case 'I':
            sprintf(id, optarg);
            rewrite_switch = 1;
            break;
        case 't':
            infinity_switch = 1;
            break;
        case 'T':
            sprintf(topic, optarg);
            rewrite_switch = 1;
            break;
        case 'U':
            sprintf(urihost, optarg);
            rewrite_switch = 1;
            break;
        default:
            printf("잘못된 명령어 인자 입력\n");
            exit(1);
        }
    }

    #ifdef _WIN32
    signal(SIGINT, handle_sigint);
    #else
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = handle_sigint;
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    /* So we do not exit on a SIGPIPE */
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, NULL);
    #endif

    if (rewrite_switch) {
        file = fopen(configroot, "w");
        sprintf(fileBuffer, "ID=%s\nTOPIC=%s\nFILENAME=%s\nFILEROOT=%s\nLOGROOT=%s\nURIHOST=%s\n\n\n\n ID(RFD 기기의 고유값)\n TOPIC(RFD가 보내고자 하는 파일의 주제)\n FILENAME(FFD에 저장될 파일명)\n FILEROOT(RFD가 보내고자 하는 파일 전체 경로)\n LOGROOT(RFD가 로그 파일을 저장할 전체 경로)\n URIHOST(FFD의 HOST주소)\n 순으로 변수명뒤에 띄어쓰기 없이 =과 텍스트를 붙임으로서 설정할 수 있으며 줄바꿈으로 값을 구별합니다.\n", id, topic, filename, fileroot, logroot, urihost);
        fputs(fileBuffer, file);
        fflush(file);
        fclose(file);
    }

	/* Required before calling other mosquitto functions */
	mosquitto_lib_init();

	/* Create a new client instance.
	 * id = NULL -> ask the broker to generate a client id for us
	 * clean session = true -> the broker should remove old sessions when we connect
	 * obj = NULL -> we aren't passing any of our private data for callbacks
	 */
	mosq = mosquitto_new(NULL, true, NULL);
	if (mosq == NULL) {
		fprintf(stderr, "Error: Out of memory.\n");
		return 1;
	}

	/* Configure callbacks. This should be done before connecting ideally. */
	mosquitto_connect_callback_set(mosq, on_connect);
	mosquitto_subscribe_callback_set(mosq, on_subscribe);
	mosquitto_publish_callback_set(mosq, on_publish);
	mosquitto_message_callback_set(mosq, on_message);

    file = fopen(fileroot, "r");
    if (file == NULL) {
        printf("파일 없음\n");
        quit = 1;
    }
    else {
        while (1) {
            fgets(fileBuffer, sizeof(fileBuffer), file);
            maxnum++;
            if (feof(file)) { break; }
        }
        fclose(file);
    }
    if (exit_switch) {
        printf("FFD 원격 종료 시작\n");//태그
    }
    else {
        printf("ID : %s\nTOPIC : %s\nFILENAME : %s\nMAXIMUM TOGGLE : %d\n", id, topic, filename, maxnum);
    }

    if (infinity_switch) {
        printf("INFINITY MODE\n");
        filename_cut = strtok(filename, ".");
        sprintf(filename_origin, filename_cut);
        sprintf(filename, "%s_%d.csv", filename_origin, infinity_count);
    }

	/* Connect to test.mosquitto.org on port 1883, with a keepalive of 60 seconds.
	 * This call makes the socket connection only, it does not complete the MQTT
	 * CONNECT/CONNACK flow, you should use mosquitto_loop_start() or
	 * mosquitto_loop_forever() for processing net traffic. */
	rc = mosquitto_connect(mosq, urihost, 1883, 60);
	if (rc != MOSQ_ERR_SUCCESS) {
		mosquitto_destroy(mosq);
		fprintf(stderr, "Error: %s\n", mosquitto_strerror(rc));
		return 1;
	}
    MakeDirectory(logroot);

    if (exit_switch) {
        sprintf(payload, "EXIT");
    }
    else if (stat_switch) {
        file = fopen(statroot, "r");
        if (file == NULL) {
            stat_switch = 0;
        }
        else {
            if (!(fgets(fileBuffer, sizeof(fileBuffer), file) == NULL) && ((QUALITY_CUT = strtok(fileBuffer, "=")) != NULL) && ((QUALITY_CUT = strtok(NULL, "=")) != NULL) && ((SIGNAL_LEVEL_CUT = strtok(NULL, "=")) != NULL) && ((QUALITY_CUT = strtok(QUALITY_CUT, " ")) != NULL) && ((SIGNAL_LEVEL_CUT = strtok(SIGNAL_LEVEL_CUT, " ")) != NULL)) {
                sprintf(payload, "READYANDSTAT:%s:%s:%s:%d:%s:%s", id, topic, filename, maxnum, QUALITY_CUT, SIGNAL_LEVEL_CUT);
                sprintf(logbuf, "ID : %s, TOPIC : %s, FILENAME : %s, MAXNUM : %d, NETWORK QUALITY : %s, NETWORK SIGNAL LEVEL : %s dBm\n", id, topic, filename, maxnum, QUALITY_CUT, SIGNAL_LEVEL_CUT);
                printf("NETWORK QUALITY : %s\nNETWORK SIGNAL LEVEL : %s dBm\n", QUALITY_CUT, SIGNAL_LEVEL_CUT);
                logfile = fopen(logroot, "a");
                fputs(logbuf, logfile);
                fflush(logfile);
                fclose(logfile);
            }
            else {
                stat_switch = 0;
            }
            fclose(file);
        }
    }
    else {
        sprintf(payload, "READY:%s:%s:%s:%d", id, topic, filename, maxnum);
    }

    sprintf(filetopic, "FILE/%s", topic);

	/* Run the network loop in a blocking call. The only thing we do in this
	 * example is to print incoming messages, so a blocking call here is fine.
	 *
	 * This call will continue forever, carrying automatic reconnections if
	 * necessary, until the user calls mosquitto_disconnect().
	 */
    while (!quit) {
        while (exit_switch && !quit) {
            if (count >= exit_count) {
                printf("FFD 원격 종료 시도중\n");//태그
                mosquitto_publish(mosq, NULL, "COMMAND", strlen(payload), payload, 0, false);
                count = 0;
            }
            mosquitto_loop(mosq, -1, 1);
            count++;
        }
        while (!start_switch && !delete_switch && !quit) {
            if (count >= start_count) {
                printf("READY\n");//태그
                mosquitto_publish(mosq, NULL, "COMMAND", strlen(payload), payload, 0, false);
                count = 0;
            }
            mosquitto_loop(mosq, -1, 1);
            count++;
        }
        while (!delete_switch && !retry_switch && !quit) {
            file = fopen(fileroot, "r");
            number = 1;
            restart_switch = 0;
            if (file == NULL) {
                printf("파일 없음\n");
                quit = 1;
                break;
            }
            while (!retry_switch && !quit) {
                if (fgets(fileBuffer, sizeof(fileBuffer), file) == NULL) { break; }
                if (number >= toggle) {
                    sprintf(payload, "%s:%d:%s", id, number, fileBuffer);
                    mosquitto_publish(mosq, NULL, filetopic, strlen(payload), payload, 0, false);
                    mosquitto_loop(mosq, 0, 1);
                    if (!timer) {
                        #ifdef _WIN32
                        startc = clock();
                        #else
                        gettimeofday(&startc, NULL);
                        #endif
                        time(&ptime);
                        pinfo = localtime(&ptime);
                        logfile = fopen(logroot, "a");
                        sprintf(logbuf, "START : %s", asctime(pinfo));
                        printf(logbuf);
                        fputs(logbuf, logfile);
                        fflush(logfile);
                        fclose(logfile);
                        difnum = (maxnum * pernum) / 10;
                        timer = 1;
                    }
                    else if (pernum < 10 && difnum == number) {
                        printf("UPLOAD : %d%%(%d line)\n", pernum * 10, difnum);
                        pernum++;
                        difnum = (maxnum * pernum) / 10;
                    }
                }
                number++;
                /*
                if (stop_switch) {
                    sprintf(payload, "OK:%s", id);
                    do {
                        if (count >= ok_count) {
                            mosquitto_publish(mosq, NULL, "COMMAND", strlen(payload), payload, 0, false);
                            count = 0;
                        }
                        mosquitto_loop(mosq, -1, 1);
                        count++;
                    } while (!restart_switch && !retry_switch && !quit);
                    number = 1;
                    stop_switch = 0;
                    restart_switch = 0;
                    if (retry_switch || quit) { break; }
                    fclose(file);
                    file = fopen(fileroot, "r");
                    restart_count++;
                }
                */
            }
            /*
            if (retry_switch || quit) {
                break;
            }
            */
            sprintf(payload, "DONE:%s:%d", id, number);
            re_switch = 1;
            count = 1000;
            while (!delete_switch && !restart_switch && !retry_switch && !quit) {
                if (count >= done_count) {
                    mosquitto_publish(mosq, NULL, "COMMAND", strlen(payload), payload, 0, false);
                    count = 0;
                }
                mosquitto_loop(mosq, -1, 1);
                count++;
            }
            re_switch = 0;
            if (restart_switch) { 
                fclose(file);
                restart_count++;
            }
        }
        if (infinity_switch && delete_switch) {
            delete_switch = 0;
            restart_count = 0;
            infinity_count++;
            sprintf(filename, "%s_%d.csv", filename_origin, infinity_count);
        }
        if (delete_switch) {
            sprintf(payload, "EMPTY:%s", id);
        }
        while (delete_switch && !quit) {
            if (count >= empty_count) {
                mosquitto_publish(mosq, NULL, "COMMAND", strlen(payload), payload, 0, false);
                count = 0;
            }
            mosquitto_loop(mosq, -1, 1);
            count++;
        }
        sprintf(payload, "READY:%s:%s:%s:%d", id, topic, filename, maxnum);
        timer = 0;
        pernum = 1;
        start_switch = 0;
        retry_switch = 0;
        fclose(file);
    }
    printf("MQTT 종료\n");

	mosquitto_lib_cleanup();
	return 0;
}

