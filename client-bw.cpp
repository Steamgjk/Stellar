//tcpclient.cc
#include <iostream>
#include <cstring>
#include <strings.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstdio>

using namespace std;

int main(int argc, char *argv[])
{
    //创建套接字
    int sk = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_port = htons(atoi(argv[2]));
    server.sin_addr.s_addr = inet_addr(argv[1]);
    //连接服务器
    connect(sk, (struct sockaddr*)&server, sizeof(server));

    char buff[4096] = {'\0'};
    //接收数据
    int ret = -1;
    size_t sum_len = 0;

    struct timeval t1, t2;
    double timeuse;


    bool first = true;
    int cnt = 0;
    while (1 == 1)
    {
        ret = recv(sk, buff, sizeof(buff), 0);
        if (first)
        {
            first = false;
            gettimeofday(&t1, NULL);
        }
        sum_len += ret;
        if (sum_len >= 400000000)
        {
            gettimeofday(&t2, NULL);
            timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec) / 1000000.0;

            double rate = (sum_len * 8) / timeuse / 1000000;

            printf("rate = %lf\n", rate);

            sum_len = 0;
            gettimeofday(&t1, NULL);
            cnt++;
            if (cnt == 10)
            {
                break;
            }
            //break;
        }
    }


    //cout << buff << endl;
    //发送数据
    //send(sk, "I am hahaya", strlen("I am hahaya") + 1, 0);

    //关闭套接字
    close(sk);
    return 0;
}