//
//  main.cpp
//  linux_socket_api
//
//  Created by Jinkun Geng on 18/05/11.
//  Copyright (c) 2016年 Jinkun Geng. All rights reserved.
//
#include "stellar_common.h"
using namespace std;

void LoadData();
void WriteLog(Block&Pb, Block&Qb, int iter_cnt);
int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);
void partitionP(Block* Pblocks);
void partitionQ(Block* Qblocks);
float CalcRMSE();
void LoadTestRating();
bool isReady(int block_id, int required_iter, int send_fd);
int genActivePushfd(int send_thread_id);
bool curIterFin(int curIter);
void ps_push();
void splice_send(int send_fd, char* buf, int len);
void PeriodicStatistics();


bool waitfor = false;
int WORKER_NUM = 1;
char* local_ips[CAP] = {"12.12.10.18", "12.12.10.18", "12.12.10.18", "12.12.10.18"};
int local_ports[CAP] = {4411, 4412, 4413, 4414};
char* remote_ips[CAP] = {"12.12.10.12", "12.12.10.15", "12.12.10.19", "12.12.10.17"};
int remote_ports[CAP] = {5511, 5512, 5513, 5514};
struct Block Pblocks[CAP];
struct Block Qblocks[CAP];
atomic_int recvCount(0);
bool canSend[CAP] = {false};

bool sendConnected[100];
bool recvConnected[100];
std::mutex mtxes[100];
int submitted_age[100];
int pushed_age[100];
int num_lens[100];

int iter_t = 0;
int iter_thresh = 10;
float alpha = 0.9;
float beta = 0.5;
float send_timestamp[100];
float recv_timestamp[100];
float estimated_arrival_time[100];
float dependency_s[100];
priority_queue<PriorityE> priorQu;
mutex qu_mtx;

std::vector<PageRankNode> pn_vec;
std::vector<int> depended_ids[100];

std::vector<int>to_send_ids[100];
int send_fds[100];



int main(int argc, const char * argv[])
{
    //ofstream ofs(LOG_FILE, ios::trunc);
    char* lip  = "127.0.0.1";
    for (int i = 0; i < CAP; i++)
    {
        local_ips[i] = lip;
        remote_ips[i] = lip;
    }
    for (int i = 0; i < CAP; i++)
    {
        local_ports[i] = 10000 + i;
        remote_ports[i] = 20000 + i;
    }
    for (int i = 0; i < WORKER_NUM; i++)
    {
        submitted_age[i] = 0;
    }
    //gen P and Q
    if (argc == 2)
    {
        WORKER_NUM = atoi(argv[1]) ;
    }
    for (int i = 0; i < PG_NUM; i++)
    {
        pn_vec.push_back(PageRankNode());
    }
    printf("Loading data\n");
    LoadData();
    printf("Load Complete\n");
    int block_units = PG_NUM / WORKER_NUM;
    for (int i = 0; i < WORKER_NUM; i++)
    {
        num_lens[i] = i * block_units;
    }
    num_lens[WORKER_NUM] = PG_NUM;

    for (int i = 0; i < WORKER_NUM; i++)
    {
        for (int j = num_lens[i]; j < num_lens[i + 1]; j++)
        {
            for (int k = 0; k < pn_vec[j].from_adj_nodes.size(); k++)
            {
                if (pn_vec[j].from_adj_nodes[k] < num_lens[i] || pn_vec[j].from_adj_nodes[k] >= num_lens[i + 1])
                {
                    depended_ids[i].push_back(pn_vec[j].from_adj_nodes[k]);
                }

            }

        }
    }
    for (int i = 0; i < WORKER_NUM; i++)
    {
        printf("%d-%d\n", depended_ids[i].size(), num_lens[i + 1] - num_lens[i]);
    }

    for (int td = 0; td < WORKER_NUM;  td++)
    {
        recvConnected[td] = false;
        sendConnected[td] = false;
    }
    waitfor = true;
    for (int recv_thread_id = 0; recv_thread_id < WORKER_NUM; recv_thread_id++)
    {
        int thid = recv_thread_id;
        printf("thid=%d\n", thid );
        std::thread recv_thread(recvTd, thid);
        recv_thread.detach();
    }
#ifndef STELLAR
    for (int send_thread_id = 0; send_thread_id < WORKER_NUM; send_thread_id++)
    {
        int thid = send_thread_id;
        std::thread send_thread(sendTd, thid);
        send_thread.detach();
    }
#endif

#ifdef STELLAR
    for (int send_thread_id = 0; send_thread_id < WORKER_NUM; send_thread_id++)
    {
        send_fds[send_thread_id] = -1;
    }
    for (int send_thread_id = 0; send_thread_id < WORKER_NUM; send_thread_id++)
    {
        sendConnected[send_thread_id] = false;
        int thid = send_thread_id;
        std::thread send_thread(genActivePushfd, thid);
        send_thread.detach();
    }

    std::thread ps_push_td(ps_push);
    ps_push_td.detach();

#endif
    std::thread periodic_td(PeriodicStatistics);
    periodic_td.detach();
    iter_t = 0;
    while (1 == 1)
    {
        if (!curIterFin(iter_t))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        iter_t++;
    }
    return 0;
}


void LoadData()
{
    int from_node, to_node;
    ifstream ifs(PS_FILE);
    if (!ifs.is_open())
    {
        printf("fail-LoadD4 to open %s\n", FILE_NAME );
        exit(-1);
    }
    //the first line is text
    string str;
    getline(ifs, str);
    int line_cnt = 0;
    while (!ifs.eof())
    {
        ifs >> from_node >> to_node;
        pn_vec[from_node].score = 0;
        pn_vec[from_node].previous_score = 0;
        pn_vec[from_node].data_age = 0;
        pn_vec[from_node].to_adj_nodes.push_back(to_node);

        pn_vec[to_node].score = 0;
        pn_vec[to_node].previous_score = 0;
        pn_vec[to_node].data_age = 0;
        pn_vec[to_node].from_adj_nodes.push_back(from_node);

        line_cnt++;
        if (line_cnt % 1000000 == 0)
        {
            printf("Loading...%d\n", line_cnt );
        }
    }

}


void PeriodicStatistics()
{
    while (1 == 1)
    {
        bool canbreak = true;
        for (int i = 0; i < WORKER_NUM; i++)
        {
            if (sendConnected[i] == false)
            {
                canbreak = false;
            }
            if (recvConnected[i] == false)
            {
                canbreak = false;
            }
        }
        if (canbreak)
        {
            waitfor = false;
            break;
        }
    }
    printf("All Connected!\n");
    ofstream ofs(LOG_FILE, ios::trunc);
    int time_units = 0;

    while (1 == 1)
    {
        printf("Start to sleep...\n");
        std::this_thread::sleep_for(std::chrono::milliseconds(50000));
        time_units++;

        waitfor = true;
        printf("Entering statistics...\n");

        float rmse = CalcRMSE();
        ofs << time_units << "\t" << iter_t << "\t" << rmse << endl;
        printf("time= %d\t rmse=%f\n", time_units, rmse );

        waitfor = false;


    }

}
float CalcRMSE()
{
    float sum = 0;
    for (int i = 0; i < PG_NUM; i++)
    {
        sum += (pn_vec[i].previous_score - pn_vec[i].score) * (pn_vec[i].previous_score - pn_vec[i].score);
    }
    float rmse = sqrt(sum / (PG_NUM));
    return rmse;
}
bool curIterFin(int curIter)
{
    if (curIter < 0)
    {
        return true;
    }
    for (int i = 0; i < WORKER_NUM; i++)
    {
        if (submitted_age[i] < curIter)
        {
            return false;
        }
    }
    return true;
}

bool isReady(int worker_id, int required_age, int fd)
{
    size_t struct_sz = sizeof(PNBlock);
    size_t data_sz = 0;
    char* buf = NULL;

    //for BSP constraints
#ifdef BSP_MODE
    if (!curIterFin(required_age))
    {
        /*
        printf("%d iter cannot send to worker %d\n", required_iter, worker_id );
        for (int ll = 0; ll < WORKER_NUM; ll++)
        {
            printf("%d\t", submitted_age[ll]);
        }
        printf("\n");
        **/
        return false;
    }

#endif

#ifdef SSP_MODE
    if (!curIterFin(required_age - SSP_BOUND) )
    {
        return false;
    }
#endif

    PNBlock pnb(PG_NUM, required_age);
    size_t idx_sz = sizeof(int) * PG_NUM;
    size_t score_sz  = sizeof(int) * PG_NUM;
    data_sz = struct_sz + idx_sz + score_sz;
    buf = (char*)malloc(data_sz);
    memcpy(buf, &pnb, struct_sz);
    int*idx_ptr = (int*)(void*)(buf + struct_sz);
    float*score_ptr = (float*)(void*)(buf + struct_sz + idx_sz);
    for (int i = 0; i < PG_NUM; i++)
    {
        idx_ptr[i] = i;
        score_ptr[i] = pn_vec[i].score / pn_vec[i].to_adj_nodes.size();
    }
    printf("send to worker %d  pnb entry_num=%d age=%d\n", worker_id, pnb.entry_num, pnb.data_age );
    splice_send(fd, buf, data_sz);
    free(buf);
    return true;
}

int genActivePushfd(int send_thread_id)
{
    printf("send_thread_id=%d\n", send_thread_id);
    char* remote_ip = remote_ips[send_thread_id];
    int remote_port = remote_ports[send_thread_id];
    int fd;
    int check_ret;
    fd = socket(PF_INET, SOCK_STREAM , 0);
    //printf("fd = %d\n", fd);
    assert(fd >= 0);

    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    //转换成网络地址
    address.sin_port = htons(remote_port);
    address.sin_family = AF_INET;
    //地址转换
    inet_pton(AF_INET, remote_ip, &address.sin_addr);
    do
    {
        check_ret = connect(fd, (struct sockaddr*) &address, sizeof(address));
        printf("[Td:%d] trying to connect %s %d\n", send_thread_id, remote_ip, remote_port );
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret < 0);
    printf("[Td:%d]connected %s  %d\n", send_thread_id, remote_ip, remote_port );
    send_fds[send_thread_id] = fd;
    sendConnected[send_thread_id] = true;
    return fd;
}

void splice_send(int send_fd, char* buf, int len)
{
    size_t to_send_len = 4096;
    size_t remain_len = len;
    size_t sent_len = 0;
    int ret = -1;
    while (remain_len > 0)
    {
        if (to_send_len > remain_len)
        {
            to_send_len = remain_len;
        }
        //printf("sending...\n");
        ret = send(send_fd, buf + sent_len, to_send_len, 0);
        if (ret >= 0)
        {
            remain_len -= to_send_len;
            sent_len += to_send_len;
            //printf("remain_len = %ld\n", remain_len);
        }
        else
        {
            printf("still fail\n");
        }
    }
}
//send only establish the fd vec, send in sequence

void ps_push()
{
    while (1 == 1)
    {
        bool ok = true;
        for (int send_td = 0; send_td < WORKER_NUM; send_td++)
        {
            if (send_fds[send_td] < 0)
            {
                ok = false;
            }
        }
        if (ok)
        {
            break;
        }
    }
    printf("start ps push\n");
    size_t struct_sz = sizeof(PNBlock);
    size_t idx_sz = -1;
    size_t score_sz = -1;
    size_t data_sz = -1;
    char* buf = NULL;

    while (1 == 1)
    {
        if (waitfor)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        int idx = -1;
        for (int i = 0; i < WORKER_NUM; i++)
        {
            for (int j = 0; j < depended_ids[i].size(); j++)
            {
                idx = depended_ids[i][j];
                if (pn_vec[idx].data_age > pushed_age[i])
                {
                    to_send_ids[i].push_back(idx);
                }
            }
            if (to_send_ids[i].size() == depended_ids[i].size())
            {
                idx_sz = sizeof(int) * (to_send_ids[i].size());
                score_sz = sizeof(float) * (to_send_ids[i].size());
                data_sz = idx_sz + score_sz + struct_sz;
                buf = (char*)malloc(data_sz);
                PNBlock pnb(to_send_ids[i].size(), pushed_age[i] + 1);
                memcpy(buf, &pnb, struct_sz);
                int* idx_ptr = (int*)(void*)(buf + struct_sz);
                float* score_ptr = (float*)(void*)(buf + struct_sz + idx_sz);
                for (int ii = 0; ii < to_send_ids[i].size(); ii++)
                {
                    int idx = to_send_ids[i][ii];
                    idx_ptr[ii] = idx;
                    score_ptr[ii] = pn_vec[idx].score;
                }
                splice_send(send_fds[i], buf, data_sz);
                free(buf);
                pushed_age[i]++;
                to_send_ids[i].clear();
            }
        }


    }
}
//recving request and then decide can/cannot sent
//corresponding to pull in worker
void sendTd(int send_thread_id)
{
    int fd = genActivePushfd(send_thread_id);
    ReqMsg* msg = (ReqMsg*)malloc(sizeof(ReqMsg));
    int ret = -1;

    while (1 == 1)
    {
        if (waitfor)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        //Stellar does not need this ReqMsg
        printf("[%d] recving request\n", send_thread_id );
        ret = recv(fd, msg, sizeof(ReqMsg), 0);

        printf("recved request %d  iter =%d\n", msg->worker_id, msg->required_iteration  );
        while (1 == 1)
        {
            if (isReady(msg->worker_id, msg->required_iteration, fd))
            {
                break;
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }

    }

}



void recvTd(int recv_thread_id)
{
    printf("recv_thread_id-11=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );
    recvConnected[recv_thread_id] = true;

    while (1 == 1)
    {
        if (waitfor)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        //printf("[%d]recving\n", recv_thread_id);
        size_t expected_len = sizeof(PNBlock);
        char* sockBuf = (char*)malloc(expected_len);
        size_t cur_len = 0;
        int ret = 0;
        while (cur_len < expected_len)
        {

            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret <=  0)
            {
                printf("[%d] Mimatch!  cur_len=%d  expected_len=%d ret=%d errno=%d\n", recv_thread_id, cur_len, expected_len, ret, errno);
                if (ret == 0)
                {
                    exit(-1);
                }
            }
            //printf("ret=%d\n", ret );
            cur_len += ret;
            //printf("cur_len=%d expected_len=%d\n", cur_len, expected_len );
        }
        struct PNBlock* pb = (struct PNBlock*)(void*)sockBuf;
        size_t idx_sz = sizeof(int) * (pb->entry_num);
        size_t score_sz = sizeof(float) * (pb->entry_num);
        size_t data_sz = idx_sz + score_sz;
        char* dataBuf = (char*)malloc(data_sz);
        cur_len = 0;
        ret = 0;
        //printf("pb ele_num %d\n", pb->ele_num );
        while (cur_len < data_sz)
        {
            ret = recv(connfd, dataBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
            // printf("cur_len=%d data_sz=%d\n", cur_len, data_sz );
        }
        int* idx_ptr = (int*)(void*)dataBuf;
        float* score_ptr = (float*)(void*)(dataBuf + idx_sz);
        for (int i = 0; i < pb->entry_num; i++)
        {
            int idx = idx_ptr[i];
            if (pn_vec[idx].data_age < pb->data_age)
            {
                pn_vec[idx].previous_score = pn_vec[idx].score;
                pn_vec[idx].score = score_ptr[i];
                pn_vec[idx].data_age = pb->data_age;
            }
        }
        submitted_age[recv_thread_id]++;
        printf("[%d]recved data  submitted_age=%d p-age=%d\n", recv_thread_id, submitted_age[recv_thread_id], pb->data_age);
        free(sockBuf);
        free(dataBuf);


    }
}

int wait4connection(char*local_ip, int local_port)
{
    int fd = socket(PF_INET, SOCK_STREAM , 0);
    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    //转换成网络地址
    address.sin_port = htons(local_port);
    address.sin_family = AF_INET;
    //地址转换
    inet_pton(AF_INET, local_ip, &address.sin_addr);
    //设置socket buffer大小
    //int recvbuf = 4096;
    //int len = sizeof( recvbuf );
    //setsockopt( fd, SOL_SOCKET, SO_RCVBUF, &recvbuf, sizeof( recvbuf ) );
    //getsockopt( fd, SOL_SOCKET, SO_RCVBUF, &recvbuf, ( socklen_t* )&len );
    //printf( "the receive buffer size after settting is %d\n", recvbuf );
    //绑定ip和端口
    int check_ret = -1;
    do
    {
        printf("binding... %s  %d\n", local_ip, local_port);
        check_ret = bind(fd, (struct sockaddr*)&address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret >= 0);

    //创建监听队列，用来存放待处理的客户连接
    check_ret = listen(fd, 5);
    assert(check_ret >= 0);
    printf("listening... %s  %d\n", local_ip, local_port);
    struct sockaddr_in addressClient;
    socklen_t clientLen = sizeof(addressClient);
    //接受连接，阻塞函数
    int connfd = accept(fd, (struct sockaddr*)&addressClient, &clientLen);
    printf("get connection-11 from %s  %d\n", inet_ntoa(addressClient.sin_addr), addressClient.sin_port);
    return connfd;

}

