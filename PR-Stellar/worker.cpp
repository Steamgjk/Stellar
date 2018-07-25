//
//  main.cpp
//  linux_socket_api
//
//  Created by Jinkun Geng
//  Copyright (c) 2016年 bikang. All rights reserved.
//
#include "stellar_common.h"
using namespace std;


int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);
void submf();
void WriteLog(Block&Pb, Block&Qb, int iter_cnt);
void CalcUpdt(int thread_id);
void LoadData();
int sendPullReq(int requre_iter, int fd);
int push_block(int sendfd, Block& blk);
void WaitforParas(int cur_iter);
int genPushTd(int send_thread_id);
bool NeededByOutSide(int idx);
int simple_push_block(int sendfd);


int WORKER_NUM = 1;
/**Yahoo!Music**/
double yita = 0.001;
double theta = 0.05;

char* remote_ips[CAP] = {"12.12.10.18", "12.12.10.18", "12.12.10.18", "12.12.10.18"};
int remote_ports[CAP] = {4411, 4412, 4413, 4414};

char* local_ips[CAP] = {"12.12.10.12", "12.12.10.15", "12.12.10.19", "12.12.10.17"};
int local_ports[CAP] = {5511, 5512, 5513, 5514};


int num_lens[100] = {0};
int thread_id = -1;
struct timeval start, stop, diff;
bool StartCalcUpdt[100];

std::vector<int> outside_vec;
std::vector<float> new_scores;
std::vector<PageRankNode> pn_vec;
int iter_cnt = 0;
int worker_id = 0;
int pull_fd, push_fd;
int recved_data_age;

int main(int argc, const char * argv[])
{
    char* lip  = "127.0.0.1";
    for (int i = 0; i < CAP; i++)
    {
        local_ips[i] = lip;
        remote_ips[i] = lip;
    }
    for (int i = 0; i < CAP; i++)
    {
        local_ports[i] = 20000 + i;
        remote_ports[i] = 10000 + i;
    }
    int thresh_log = 1200;
    if (argc >= 2)
    {
        thread_id = atoi(argv[1]);
        worker_id = thread_id;
    }
    if (argc >= 3)
    {
        WORKER_NUM = atoi(argv[2]);
    }

    int block_units = PG_NUM / WORKER_NUM;
    for (int i = 0; i < WORKER_NUM; i++)
    {
        num_lens[i] = i * block_units;
    }
    num_lens[WORKER_NUM] = PG_NUM;

    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        StartCalcUpdt[i] = (false);
    }
    for (int i = 0; i < PG_NUM; i++)
    {
        new_scores.push_back(0);
        //pn_vec.push_back(PageRankNode());
    }
    for (int i = num_lens[worker_id]; i < num_lens[worker_id + 1]; i++ )
    {
        pn_vec.push_back(PageRankNode());
    }
    for (int i = 0; i < num_lens[worker_id + 1] - num_lens[worker_id]; i++ )
    {
        pn_vec[i].previous_score = 0;
        pn_vec[i].score = 0;
        pn_vec[i].data_age = 0;
    }
    printf("Init Over\n");
    for (int i = num_lens[worker_id]; i < num_lens[worker_id + 1]; i++ )
    {
        if (NeededByOutSide(i))
        {
            outside_vec.push_back(i);
        }
    }
    iter_cnt = 0;
    printf("Loading Data\n");
    LoadData();
    printf("Load Rating Success\n");
    {
        printf("recv th_id=%d\n", thread_id );
        std::thread recv_thread(recvTd, thread_id);
        recv_thread.detach();
    }
    printf("wait for you for 3s\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    {
        push_fd = genPushTd(thread_id);
    }

    std::vector<thread> td_vec;
    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        td_vec.push_back(std::thread(CalcUpdt, i));
    }
    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        td_vec[i].detach();
    }
    printf("detached well\n");

    while (1 == 1)
    {
        {
            //SGD
            printf("waiting for Paras iter_cnt=%d\n", iter_cnt);
            WaitforParas(iter_cnt);
            submf();
            printf("Pushing-A... iter  %d\n", iter_cnt );
#ifdef STELLAR
            simple_push_block(push_fd);
#else
            push_block(push_fd);
#endif
            printf("Pushed-1... iter %d\n", iter_cnt);
            iter_cnt++;
        }
    }


}

bool NeededByOutSide(int node_id)
{
    int idx = node_id - num_lens[worker_id];
    for (size_t i = 0; i < pn_vec[idx].to_adj_nodes.size(); i++)
    {
        if (pn_vec[idx].to_adj_nodes[i] < num_lens[worker_id] || pn_vec[idx].to_adj_nodes[i] >= num_lens[worker_id + 1])
        {
            return true;
        }
    }
    return false;
}
void WaitforParas(int cur_iter)
{

#ifdef BSP_MODE
    while (recved_data_age < cur_iter)
#endif
#ifdef SSP_MODE
        while (recved_data_age < cur_iter - SSP_BOUND || recved_data_age < 0)
#endif
#ifdef ASP_MODE
            while (recved_data_age < 0)
#endif
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
}
void LoadData()
{
    printf("loading data thread_id=%d\n", thread_id );
    int from_node, to_node;
    char fn[100];
    sprintf(fn, "%s%d", FILE_NAME, worker_id);
    ifstream ifs(fn);
    if (!ifs.is_open())
    {
        printf("fail-LoadD4 to open %s\n", FILE_NAME );
        exit(-1);
    }
//the first line is text
    //string str;
    //getline(ifs, str);

    int line_cnt = 0;
    while (!ifs.eof())
    {
        ifs >> from_node >> to_node;

        if (from_node >= num_lens[worker_id] && from_node < num_lens[worker_id + 1])
        {
            int idx = from_node - num_lens[worker_id];
            pn_vec[idx].to_adj_nodes.push_back(to_node);
        }
        if (to_node >= num_lens[worker_id] && to_node < num_lens[worker_id + 1])
        {
            int idx = to_node - num_lens[worker_id];
            pn_vec[idx].from_adj_nodes.push_back(to_node);
        }

        line_cnt++;
        if (line_cnt % 1000000 == 0)
        {
            printf("Loading...%d\n", line_cnt );
        }
    }

}


void WriteLog()
{
    char fn[100];
    sprintf(fn, "Block-%d-%d", iter_cnt, worker_id);
    ofstream pofs(fn, ios::trunc);
    for (int i = num_lens[worker_id]; i < num_lens[worker_id]; i++)
    {
        pofs << i << "\t" << pn_vec[i].score << "\t" << pn_vec[i].previous_score << "\t" << pn_vec[i].data_age << "\t" << (pn_vec[i].previous_score - pn_vec[i].score) << endl;
    }
}

void CalcUpdt(int td_id)
{
    while (1 == 1)
    {
        if (StartCalcUpdt[td_id] == true)
        {
            for (int i = num_lens[worker_id]; i < num_lens[worker_id + 1]; i++)
            {
                if (i % WORKER_THREAD_NUM == td_id)
                {
                    int idx = i - num_lens[worker_id];
                    float ret_score = 0;
                    for (size_t j = 0; j < pn_vec[idx].from_adj_nodes.size(); j++)
                    {
                        int from_node_id = pn_vec[idx].from_adj_nodes[j];
                        ret_score += new_scores[from_node_id];

                    }
                    new_scores[i] = 0.85 * ret_score + 0.15 * (pn_vec[idx].score);
                }
            }
            StartCalcUpdt[td_id] = false;
        }
    }
}

void submf()
{
    bool canbreak = true;

    for (int ii = 0; ii < WORKER_THREAD_NUM; ii++)
    {
        StartCalcUpdt[ii] = true;
    }
    while (1 == 1)
    {
        canbreak = true;
        for (int ii = 0; ii < WORKER_THREAD_NUM; ii++)
        {
            if (StartCalcUpdt[ii] == true)
            {
                //printf("ii=%d, %d \n", ii, StartCalcUpdt[ii] );
                canbreak = false;
            }
        }
        if (canbreak)
        {
            break;
        }
    }
    int idx = 0;
    for (int i = num_lens[worker_id]; i < num_lens[worker_id + 1]; i++)
    {
        idx = i - num_lens[worker_id];
        pn_vec[idx].previous_score = pn_vec[idx].score;
        pn_vec[idx].data_age++;
        pn_vec[idx].score = new_scores[idx];
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
    //绑定ip和端口
    int check_ret = -1;
    do
    {
        printf("binding...\n");
        check_ret = bind(fd, (struct sockaddr*)&address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret >= 0);
    printf("bind ok\n");
    //创建监听队列，用来存放待处理的客户连接
    check_ret = listen(fd, 5);
    assert(check_ret >= 0);

    struct sockaddr_in addressClient;
    socklen_t clientLen = sizeof(addressClient);

    printf("thread %d listening at %s %d\n", thread_id, local_ip, local_port );
    //接受连接，阻塞函数
    int connfd = accept(fd, (struct sockaddr*)&addressClient, &clientLen);
    return connfd;

}

int push_block(int sendfd)
{
    //printf("Td:%d cansend\n", thread_id );
    size_t struct_sz = sizeof(PNBlock);
    PNBlock pn((num_lens[worker_id + 1] - num_lens[worker_id]), iter_cnt + 1);
    size_t idx_sz = sizeof(int) * (pn.entry_num);
    size_t entry_sz = sizeof(float) * (pn.entry_num);
    size_t data_sz = idx_sz + entry_sz;

    std::vector<int> idxes;
    std::vector<float> scores;
    int idx = 0;
    for (int i = num_lens[worker_id]; i < num_lens[worker_id + 1]; i++)
    {
        idxes.push_back(i);
        idx = i - num_lens[worker_id];
        scores.push_back(pn_vec[idx].score);
    }
    char* buf = (char*)malloc(struct_sz + data_sz);
    memcpy(buf, &(pn), struct_sz);
    memcpy(buf + struct_sz, (char*) & (idxes[0]), idx_sz);
    memcpy(buf + struct_sz + idx_sz, (char*) & (scores[0]), entry_sz);

    size_t total_len = struct_sz + data_sz;
    size_t sent_len = 0;
    size_t remain_len = total_len;
    int ret = -1;
    size_t to_send_len = 4096;
    //gettimeofday(&st, 0);
    while (remain_len > 0)
    {
        if (to_send_len > remain_len)
        {
            to_send_len = remain_len;
        }
        //printf("sending...\n");
        ret = send(sendfd, buf + sent_len, to_send_len, 0);
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
        //getchar();
    }
    free(buf);
    return 0;
}

//??? Problem
int simple_push_block(int sendfd)
{
    size_t struct_sz = sizeof(PNBlock);
    PNBlock pn(outside_vec.size(), iter_cnt + 1);
    size_t idx_sz = sizeof(int) * (pn.entry_num);
    size_t entry_sz = sizeof(float) * (pn.entry_num);
    size_t data_sz = idx_sz + entry_sz;
    std::vector<float> scores;
    for (size_t i = 0; i < outside_vec.size(); i++)
    {
        int idx = outside_vec[i] - num_lens[worker_id];
        scores.push_back(pn_vec[idx].score);
    }
    char* buf = (char*)malloc(struct_sz + data_sz);
    memcpy(buf, &(pn), struct_sz);
    memcpy(buf + struct_sz, (char*) & (outside_vec[0]), idx_sz);
    memcpy(buf + struct_sz + idx_sz, (char*) & (scores[0]), entry_sz);

    size_t total_len = struct_sz + data_sz;
    size_t sent_len = 0;
    size_t remain_len = total_len;
    int ret = -1;
    size_t to_send_len = 4096;
    //gettimeofday(&st, 0);
    while (remain_len > 0)
    {
        if (to_send_len > remain_len)
        {
            to_send_len = remain_len;
        }
        //printf("sending...\n");
        ret = send(sendfd, buf + sent_len, to_send_len, 0);
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
        //getchar();
    }
    free(buf);
    return 0;
}

//push
int genPushTd(int send_thread_id)
{
    printf("send_thread_id=%d\n", send_thread_id);
    char* remote_ip = remote_ips[send_thread_id];
    int remote_port = remote_ports[send_thread_id];
    int fd;
    int check_ret;
    fd = socket(PF_INET, SOCK_STREAM , 0);
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
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret < 0);
    assert(check_ret >= 0);
    //发送数据
    printf("connect to %s %d\n", remote_ip, remote_port);
    return fd;
}

int sendPullReq(int requre_iter, int fd)
{
    ReqMsg* rm = (ReqMsg*)malloc(sizeof(ReqMsg));
    rm->required_iteration = requre_iter;
    rm->worker_id = thread_id;
    int ret = send(fd, rm, sizeof(ReqMsg), 0);
    free(rm);
    return ret;
}
//pull
void recvTd(int recv_thread_id)
{
    printf("recv_thread_id -new =%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );

    printf("[Td:%d] worker get connection\n", recv_thread_id);
    size_t struct_sz = sizeof(PNBlock);
    char* blockbuf = (char*)malloc(struct_sz);
    char* dataBuf = NULL;
    size_t data_sz = 0;
    size_t idx_sz = 0;
    size_t score_sz = 0;

    int to_recv_age = 1;
    int ret = -1;
    while (1 == 1)
    {
        printf("[%d]sending request to_recv_age=%d\n", worker_id, to_recv_age );
        ret = sendPullReq(to_recv_age, connfd);

        size_t cur_len = 0;
        printf("[%d] recving...\n", recv_thread_id );
        ret = recv(connfd, blockbuf, struct_sz, 0);
        struct PNBlock* pnb = (struct PNBlock*)(void*)blockbuf;

        idx_sz = sizeof(int) * (pnb->entry_num);
        score_sz = sizeof(float) * (pnb->entry_num);
        data_sz = idx_sz + score_sz;
        dataBuf = (char*)malloc(data_sz);
        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            //printf("recving 2\n");
            ret = recv(connfd, dataBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }
        int* idx_ptr = (int*)(void*)dataBuf;
        float* score_ptr = (float*)(void*)(dataBuf + idx_sz);
        int idx = 0;
        for (int i = 0; i < pnb->entry_num; i++)
        {
            idx -= idx_ptr[i] - num_lens[worker_id];
            if (pn_vec[idx].data_age < pnb->data_age)
            {
                pn_vec[idx].previous_score = pn_vec[idx].score;
                pn_vec[idx].score = score_ptr[i];
                pn_vec[idx].data_age = pnb->data_age;
            }
        }
        free(dataBuf);

        to_recv_age++;
    }
}



