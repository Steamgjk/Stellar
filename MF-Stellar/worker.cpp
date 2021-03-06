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

int WORKER_NUM = 1;
/**Yahoo!Music**/
double yita = 0.001;
double theta = 0.05;

char* remote_ips[CAP] = {"172.17.244.99", "172.17.244.99", "172.17.244.99", "172.17.244.99", "172.17.244.99", "172.17.244.99", "172.17.244.99", "172.17.244.99", "172.17.244.99", "172.17.244.99", "172.17.244.99", "172.17.244.99", "172.17.244.99", "172.17.244.99", "172.17.244.99", "172.17.244.99"};
int remote_ports[CAP] = {4411, 4412, 4413, 4414};

char* local_ips[CAP] = {"172.17.244.100", "172.17.244.101", "172.17.244.102", "172.17.244.103", "172.17.244.104", "172.17.244.105", "172.17.244.106", "172.17.244.107", "172.17.244.108", "172.17.244.109", "172.17.244.110", "172.17.244.111", "172.17.244.112", "172.17.244.113", "172.17.244.114", "172.17.244.115"};
int local_ports[CAP] = {5511, 5512, 5513, 5514};

int row_lens[100] = {0};
int col_lens[100] = {0};
std::vector<int> rb_ids;
std::vector<int> cb_ids;
std::vector<Entry> entry_vec[ROW_PS][COL_RS];
//obselete
struct Block Pblock;
struct Block Qblock;
/////////////////
struct Block* Pblock_ptr;
struct Block* Qblock_ptr;
struct Block Pblocks[CAP];
struct Block Qblocks[CAP];
vector<float> oldP;
vector<float> oldQ;
bool canSend = false;
bool hasRecved = false;
int thread_id = -1;
struct timeval start, stop, diff;
bool StartCalcUpdt[100];
map<long, double> RMap;
int iter_cnt = 0;
long long calcTimes[2000];
long long calc_time;
long long load_time;
long long loadTimes[2000];
int pull_fd, push_fd;

//int check_points[40] = {2, 7, 13, 18, 24, 30, 36, 41, 47, 53, 60, 65, 70, 75, 82, 87, 93, 98, 104, 110, 116, 122, 127, 133, 138, 144, 149, 156, 162, 167, 173};

int check_points[40] = {23, 49, 74, 100, 126, 152, 179, 204, 231, 257, 283, 309, 334, 361, 387, 413, 439, 465, 491, 517, 543, 569, 594, 620, 646, 671, 699, 725, 751, 777, 803};

int main(int argc, const char * argv[])
{
#ifndef TEST_BED
    char* lip  = "127.0.0.1";
    for (int i = 0; i < CAP; i++)
    {
        local_ips[i] = lip;
        remote_ips[i] = lip;
    }
#endif
    for (int i = 0; i < CAP; i++)
    {
        local_ports[i] = 20000 + i;
        remote_ports[i] = 10000 + i;
    }
    for (int i = 0; i < CAP; i++)
    {
        Pblocks[i].block_id = -1;
        Pblocks[i].data_age = -1;
        Qblocks[i].block_id = -1;
        Qblocks[i].data_age = -1;
    }
    int thresh_log = 1200;
    if (argc >= 2)
    {
        thread_id = atoi(argv[1]);
    }
    if (argc >= 3)
    {
        WORKER_NUM = atoi(argv[2]);
    }

    int row_unit = ROW_PS / WORKER_NUM;
    for (int i = 0; i < WORKER_NUM; i++)
    {
        row_lens[i] = i * row_unit;
        printf("row_lens[%d]=%d\n", i, row_lens[i] );
    }
    row_lens[WORKER_NUM] = ROW_PS;
    int col_unit = COL_RS / WORKER_NUM;
    for (int i = 0; i < WORKER_NUM; i++)
    {
        col_lens[i] = i * col_unit;
    }
    col_lens[WORKER_NUM] = COL_RS;

    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        StartCalcUpdt[i] = (false);
    }
    canSend = false;
    hasRecved = false;
    memset(&start, 0, sizeof(struct timeval));
    memset(&stop, 0, sizeof(struct timeval));
    memset(&diff, 0, sizeof(struct timeval));
    iter_cnt = 0;
    calc_time = 0;
    bool isstart = false;
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
        /*
        std::thread send_thread(sendTd, thread_id);
        send_thread.detach();
        **/
        push_fd = genPushTd(thread_id);
    }


    std::vector<thread> td_vec;
    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        //std::thread td(CalcUpdt, i);
        td_vec.push_back(std::thread(CalcUpdt, i));
    }
    for (int i = 0; i < WORKER_THREAD_NUM; i++)
    {
        td_vec[i].detach();
    }
    printf("detached well\n");

    int log_idx = 0;
    while (1 == 1)
    {
        //printf(" hasRecved? %d\n", hasRecved);
        //std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        //if (hasRecved)
        {
            //printf("starting iter_cnt=%d\n", iter_cnt);
            if (!isstart)
            {
                isstart = true;
                gettimeofday(&start, 0);
            }

            //SGD

            printf("waiting for Paras iter_cnt=%d\n", iter_cnt);

            WaitforParas(iter_cnt);

            //printf("Computing... iter %d pbid=%d qbid=%d page=%d  qage=%d\n", iter_cnt, Pblock_ptr->block_id, Qblock_ptr->block_id, Pblock_ptr->data_age, Qblock_ptr->data_age);
            submf();

            //printf("Pushing... iter  %d\n", iter_cnt );

#ifndef STELLAR
            push_block(push_fd, (*Pblock_ptr));
#endif
            push_block(push_fd, (*Qblock_ptr));
//#ifdef DEBUG
            printf("Pushed... iter11 %d\n", iter_cnt);
//#endif

            if (log_idx <= 30)
            {
                printf("iter_cnt=%d check_points=%d\n", iter_cnt, check_points[log_idx] );
                if (iter_cnt == check_points[log_idx])
                {
                    WriteLog(*Pblock_ptr, *Qblock_ptr, iter_cnt);
                    log_idx++;
                }
            }
            iter_cnt++;

            canSend = true;
            //printf("canSend = true\n");
            hasRecved = false;

        }
    }


}

void WaitforParas(int cur_iter)
{
    int pbid = thread_id;
    int qbid = (thread_id + cur_iter) % WORKER_NUM;
#ifndef STELLAR
#ifdef DEBUG
    printf("waiting for pid=%d  data-age=%d  cur_iter=%d\n", pbid, Pblocks[pbid].data_age, cur_iter  );
#endif
#endif
#ifdef BSP_MODE
    while (Pblocks[pbid].data_age < cur_iter)
#endif
#ifdef SSP_MODE
        while (Pblocks[pbid].data_age < cur_iter - SSP_BOUND || Pblocks[pbid].data_age < 0)
#endif
#ifdef ASP_MODE
            while (Pblocks[pbid].data_age < 0)
#endif
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }


    Pblock_ptr = &(Pblocks[pbid]);
#ifdef DEBUG
    printf("waiting for qid=%d  data-age =%d  cur_iter=%d\n", qbid, Qblocks[qbid].data_age, cur_iter );
#endif

#ifdef BSP_MODE
    while (Qblocks[qbid].data_age < cur_iter)
#endif
#ifdef SSP_MODE
        while (Qblocks[qbid].data_age < cur_iter - SSP_BOUND || Qblocks[qbid].data_age < 0)
#endif
#ifdef ASP_MODE
            while (Qblocks[qbid].data_age < 0)
#endif

            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }



    //printf("all get the qbid=%d\n", qbid);
    //printf("qbloc = %p\n",  &(Qblocks[qbid]));
    Qblock_ptr = &(Qblocks[qbid]);
}
void LoadData()
{
    /*
    for (int i = 0; i < 100; i ++)
    {
        for (int j = 0; j < 100; j++)
        {
            entry_vec[i][j].clear();
        }
    }
    **/
    printf("loading data thread_id=%d  len_sta %d len_end  %d\n", thread_id, row_lens[thread_id], row_lens[thread_id + 1] );
    char fn[100];
    int user_id, movie_id;
    float rate;
    for (int row = row_lens[thread_id]; row < row_lens[thread_id + 1]; row++)
    {
        for (int col = 0; col < COL_RS; col++)
        {
            sprintf(fn, "%s%d-%d", FILE_NAME, row, col);
            ifstream ifs(fn);
            if (!ifs.is_open())
            {
                printf("fail-LoadD4 to open %s\n", fn );
                exit(-1);
            }
            while (!ifs.eof())
            {
                user_id = -1;
                ifs >> user_id >> movie_id >> rate;
                //scale
                rate = rate / 100;
                if (user_id >= 0)
                {
                    Entry e(user_id, movie_id, rate);
                    entry_vec[row][col].push_back(e);
                }
            }
        }
        printf("row=%d\n", row );
    }

}


void WriteLog(Block&Pb, Block&Qb, int iter_cnt)
{
    char fn[100];
    printf("Writing log  iter=%d\n", iter_cnt );
    sprintf(fn, "./PS-track/Pblock-%d-%d", iter_cnt, Pb.block_id);
    ofstream pofs(fn, ios::trunc);
    for (int h = 0; h < Pb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            pofs << Pb.eles[h * K + j] << " ";
        }
        pofs << endl;
    }
    sprintf(fn, "./PS-track/Qblock-%d-%d", iter_cnt, Qb.block_id);
    ofstream qofs(fn, ios::trunc);
    for (int h = 0; h < Qb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            qofs << Qb.eles[h * K + j] << " ";
        }
        qofs << endl;
    }
}

void CalcUpdt(int td_id)
{
    while (1 == 1)
    {
        if (StartCalcUpdt[td_id] == true)
        {
            //printf("enter CalcUpdt\n");
            int times_thresh = 5000;
            //printf("rb_ids.sz=%ld cb_ids.sz=%ld\n", rb_ids.size(), cb_ids.size() );
            int p_block_idx = rb_ids[td_id];
            int q_block_idx = cb_ids[td_id];
            size_t block_sz = entry_vec[p_block_idx][q_block_idx].size();
#ifdef DEBUG
            printf("CalcUpdt[%d] pid=%d qid=%d block_sz=%ld\n", td_id, p_block_idx, q_block_idx, block_sz );
#endif

            if (block_sz == 0)
            {
                StartCalcUpdt[td_id] = false;
                continue;
            }
            int rand_idx = -1;
            for (int tr = times_thresh; tr > 0; tr--)
            {
                rand_idx = random() % block_sz;
                int movie_id = entry_vec[p_block_idx][q_block_idx][rand_idx].movie_id;
                int user_id = entry_vec[p_block_idx][q_block_idx][rand_idx].user_id;
                float rate = entry_vec[p_block_idx][q_block_idx][rand_idx].rate;
                int i = user_id - Pblock_ptr->sta_idx;
                int j = movie_id - Qblock_ptr->sta_idx;
                float error = rate;

                for (int k = 0; k < K; ++k)
                {

                    oldP[i * K + k] = (oldP[i * K + k] > 0.5) ? (0.5) : (oldP[i * K + k]);
                    oldP[i * K + k] = (oldP[i * K + k] < -0.5) ? (-0.5) : (oldP[i * K + k]);
                    oldQ[j * K + k] = (oldQ[j * K + k] > 0.5) ? (0.5) : (oldQ[j * K + k]);
                    oldQ[j * K + k] = (oldQ[j * K + k] < -0.5) ? (-0.5) : (oldQ[j * K + k]);
                    error -= oldP[i * K + k] * oldQ[j * K + k];
                }

                for (int k = 0; k < K; ++k)
                {
                    Pblock_ptr->eles[i * K + k] += yita * (error * oldQ[j * K + k] - theta * oldP[i * K + k]);
                    Qblock_ptr->eles[j * K + k] += yita * (error * oldP[i * K + k] - theta * oldQ[j * K + k]);

                    Pblock_ptr->eles[i * K + k] = (Pblock_ptr->eles[i * K + k] > 0.5) ? (0.5) : (Pblock_ptr->eles[i * K + k]);
                    Pblock_ptr->eles[i * K + k] = (Pblock_ptr->eles[i * K + k] < -0.5) ? (-0.5) : (Pblock_ptr->eles[i * K + k]);

                    Qblock_ptr->eles[j * K + k] = (Qblock_ptr->eles[j * K + k] > 0.5) ? (0.5) : (Qblock_ptr->eles[j * K + k]);
                    Qblock_ptr->eles[j * K + k] = (Qblock_ptr->eles[j * K + k] < -0.5) ? (-0.5) : (Qblock_ptr->eles[j * K + k]);

                    float tmp = Pblock_ptr->eles[i * K + k];
                    /*
                    if (Pblock_ptr->eles[i * K + k] + 1 == Pblock_ptr->eles[i * K + k] - 1)
                    {
                        //printf("p %d q %d  error =%lf rate=%lf i=%d j=%d k=%d rand_idx=%d user_id=%d  movie_id=%d  pval=%f  qval=%f\n", p_block_idx, q_block_idx, error, rate, i, j, k, rand_idx,  user_id, movie_id, Pblock_ptr->eles[i * K + k], Qblock_ptr->eles[j * K + k] );

                        if (Pblock_ptr->eles[i * K + k] > 0.5)
                        {
                            printf("pval>0.5  pval=%f  tmp=%f\n", Pblock_ptr->eles[i * K + k], tmp);
                        }
                        if (Pblock_ptr->eles[i * K + k] < -0.5)
                        {
                            printf("pval<-0.5\n");
                        }
                        if (Qblock_ptr->eles[j * K + k] > 0.5)
                        {
                            printf("qval>0.5\n");
                        }
                        if (Qblock_ptr->eles[j * K + k] < -0.5)
                        {
                            printf("pval<-0.5\n");
                        }
                        getchar();
                    }
                    **/
                }

            }
            //printf("Fini %d\n", td_id);
            StartCalcUpdt[td_id] = false;
        }
    }
}

void submf()
{
    int pid = Pblock_ptr->block_id;
    int qid = Qblock_ptr->block_id - WORKER_NUM;
    /*
        for (int ii = 0; ii < Pblock_ptr->ele_num; ii++)
        {
            if (Pblock_ptr->eles[ii] > 0.5)
            {
                Pblock_ptr->eles[ii] = 0.5;
            }
            if (Pblock_ptr->eles[ii] < -0.5)
            {
                Pblock_ptr->eles[ii] = -0.5;
            }
        }
        for (int jj = 0; jj < Qblock_ptr->ele_num; jj++)
        {
            if (Qblock_ptr->eles[jj] > 0.5)
            {
                Qblock_ptr->eles[jj] = 0.5;
            }
            if (Qblock_ptr->eles[jj] < -0.5)
            {
                Qblock_ptr->eles[jj] = -0.5;
            }
        }
        **/
    oldP = Pblock_ptr->eles;
    oldQ = Qblock_ptr->eles;

    rb_ids.clear();
    cb_ids.clear();
    for (int i = row_lens[pid]; i < row_lens[pid + 1]; i++)
    {
        rb_ids.push_back(i);
    }
    for (int j = col_lens[qid]; j < col_lens[qid + 1]; j++)
    {
        cb_ids.push_back(j);
    }
    //printf("A rb_ids.size=%ld  cb_ids.sz=%ld\n", rb_ids.size(), cb_ids.size() );
    random_shuffle(rb_ids.begin(), rb_ids.end()); //迭代器
    random_shuffle(cb_ids.begin(), cb_ids.end()); //迭代器
#ifdef DEBUG
    for (size_t i = 0; i < rb_ids.size(); i++)
    {
        printf("%d %d\n", rb_ids[i], cb_ids[i]);
    }
#endif

    struct timeval beg, ed;
    long long mksp;
    gettimeofday(&beg, 0);
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

    /*
        for (size_t i = 0; i < oldP.size(); i++)
        {

            Pblock_ptr->eles[i] = (Pblock_ptr->eles[i] > 0.5) ? (0.2) : (Pblock_ptr->eles[i]);
            Pblock_ptr->eles[i] = (Pblock_ptr->eles[i] < -0.5) ? (0) : (Pblock_ptr->eles[i]);
            //Pblock_ptr->eles[i] -= oldP[i];


        }
        for (size_t i = 0; i < oldQ.size(); i++)
        {
            Qblock_ptr->eles[i] = (Qblock_ptr->eles[i] > 0.5) ? (0.2) : (Qblock_ptr->eles[i]);
            Qblock_ptr->eles[i] = (Qblock_ptr->eles[i] < -0.5) ? (0) : (Qblock_ptr->eles[i]);
            //Qblock_ptr->eles[i] -= oldQ[i];

        }
    **/
#if (defined STELLAR)||(defined SSP_MODE)
    Pblock_ptr->data_age++;
    Qblock_ptr->data_age++;
#endif
    gettimeofday(&ed, 0);
    mksp = (ed.tv_sec - beg.tv_sec) * 1000000 + ed.tv_usec - beg.tv_usec;
    //printf("Calc  time = %lld\n", mksp);
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

int push_block(int sendfd, Block& blk)
{
    //printf("Td:%d cansend\n", thread_id );
    size_t struct_sz = sizeof(Block);
    size_t data_sz = sizeof(float) * blk.ele_num;
    char* buf = (char*)malloc(struct_sz + data_sz);
    memcpy(buf, &(blk), struct_sz);
    memcpy(buf + struct_sz, (char*) & (blk.eles[0]), data_sz);
    size_t total_len = struct_sz + data_sz;
    //printf("total_len=%ld struct_sz=%ld data_sz=%ld  elenum=%d\n", total_len, struct_sz, data_sz, blk.ele_num );

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
    size_t struct_sz = sizeof(Block);
    char* blockbuf = (char*)malloc(struct_sz);
    char* dataBuf = NULL;
    size_t data_sz = 0;
    int to_recv_cnt = 0;
    int has_request_cnt = -1;
    bool one_p = false;
    bool one_q = false;

    while (1 == 1)
    {
        //printf("to_recv_cnt=%d has_request_cnt=%d iter_cnt=%d\n", to_recv_cnt, has_request_cnt, iter_cnt );
#ifndef STELLAR
        if (to_recv_cnt > iter_cnt)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
#endif
        struct timeval st, et;
        gettimeofday(&st, 0);
        int ret = -1;
#ifndef STELLAR
        if (has_request_cnt < to_recv_cnt)
        {
            printf("[%d]send request %d\n", recv_thread_id, to_recv_cnt );

            // In stellar, we donot need active pull
            ret = sendPullReq(to_recv_cnt, connfd);
            has_request_cnt++;
        }
#endif
#ifdef STELLAR
        one_p = true;
#endif
        size_t cur_len = 0;
#ifdef DEBUG
        printf("[%d] recving...\n", recv_thread_id );
#endif
        ret = recv(connfd, blockbuf, struct_sz, 0);
        struct Block* pb = (struct Block*)(void*)blockbuf;
        data_sz = sizeof(float) * (pb->ele_num);
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

        if (pb->block_id < WORKER_NUM)
        {
#ifdef DEBUG
            printf("recved Pblock bid=%d  age=%d\n", pb->block_id, pb->data_age);
#endif
            //is Pblock
            int pbid = pb->block_id;
            Pblocks[pbid].block_id = pbid;
            Pblocks[pbid].sta_idx = pb->sta_idx;
            Pblocks[pbid].height = pb->height;
            Pblocks[pbid].ele_num = pb->ele_num;
            Pblocks[pbid].eles.resize(pb->ele_num);
            float* data_eles = (float*)(void*)dataBuf;
            if (pb->data_age > Pblocks[pbid].data_age)
            {
                for (int i = 0; i < Pblocks[pbid].ele_num; i++)
                {
                    Pblocks[pbid].eles[i] = data_eles[i];
                }
                Pblocks[pbid].data_age = pb->data_age;
            }


            one_p = true;
        }
        else
        {
#ifdef DEBUG
            printf("recved Qblock bid=%d  age=%d\n", pb->block_id, pb->data_age);
#endif
            int qbid = pb->block_id - WORKER_NUM;
            Qblocks[qbid].block_id = pb->block_id;
            Qblocks[qbid].sta_idx = pb->sta_idx;
            Qblocks[qbid].height = pb->height;
            Qblocks[qbid].ele_num = pb->ele_num;
            Qblocks[qbid].eles.resize(pb->ele_num);
            float* data_eles = (float*)(void*)dataBuf;
            if (pb->data_age > Qblocks[qbid].data_age)
            {
                for (int i = 0; i < Qblocks[qbid].ele_num; i++)
                {
                    Qblocks[qbid].eles[i] = data_eles[i];
                }
                Qblocks[qbid].data_age = pb->data_age;
            }

            one_q = true;
        }
        free(dataBuf);
        if (one_p && one_q)
        {
#ifdef DEBUG
            printf("received paras for iter %d\n", to_recv_cnt );
            printf("qbid=%d  data-age = %d\n", pb->block_id - WORKER_NUM, Qblocks[pb->block_id - WORKER_NUM].data_age );
#endif
            to_recv_cnt++;
            one_p = false;
            one_q = false;

        }


        gettimeofday(&et, 0);
        long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;

        hasRecved = true;
    }
}




///////////////////////////////////

void sendTd1(int send_thread_id)
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
    while (1 == 1)
    {
        //printf("canSend=%d\n", canSend );
        if (canSend)
        {
            printf("Td:%d cansend\n", thread_id );
            size_t struct_sz = sizeof(Block);
            size_t data_sz = sizeof(float) * Pblock.ele_num;
            char* buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Pblock), struct_sz);
            memcpy(buf + struct_sz, (char*) & (Pblock.eles[0]), data_sz);

            size_t total_len = struct_sz + data_sz;
            printf("total_len=%ld struct_sz=%ld data_sz=%ld  elenum=%d\n", total_len, struct_sz, data_sz, Pblock.ele_num );
            //struct timeval st, et, tspan;
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
                ret = send(fd, buf + sent_len, to_send_len, 0);
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
            data_sz = sizeof(float) * Qblock.ele_num;
            total_len = struct_sz + data_sz;
            buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Qblock), struct_sz);
            memcpy(buf + struct_sz , (char*) & (Qblock.eles[0]), data_sz);
            printf("Q  total_len=%ld struct_sz=%ld data_sz=%ld ele_num=%d\n", total_len, struct_sz, data_sz, Qblock.ele_num );
            sent_len = 0;
            remain_len = total_len;
            ret = -1;
            to_send_len = 4096;
            while (remain_len > 0)
            {
                if (to_send_len > remain_len)
                {
                    to_send_len = remain_len;
                }
                //printf("sending...\n");
                ret = send(fd, buf + sent_len, to_send_len, 0);
                if (ret >= 0)
                {
                    remain_len -= to_send_len;
                    sent_len += to_send_len;
                }
                else
                {
                    printf("still fail\n");
                }
            }

            free(buf);
            /*
            gettimeofday(&et, 0);
            long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
            printf("send two blocks mksp=%lld\n", mksp );
            **/
            canSend = false;
        }

    }

}

void recvTd1(int recv_thread_id)
{
    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );

    printf("[Td:%d] worker get connection\n", recv_thread_id);
    while (1 == 1)
    {
        //printf("recv loop\n");
        struct timeval st, et;
        gettimeofday(&st, 0);
        size_t expected_len = sizeof(Pblock);
        char* sockBuf = (char*)malloc(expected_len + 100);
        size_t cur_len = 0;
        int ret = 0;
        printf("recvTd check 1 expected_len=%ld\n", expected_len);
        while (cur_len < expected_len)
        {
            printf("recving..\n");

            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            printf("check 1.5\n");
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }
        printf("recvTd check 2\n");
        struct Block* pb = (struct Block*)(void*)sockBuf;
        Pblock.block_id = pb->block_id;
        Pblock.data_age = pb->data_age;
        Pblock.sta_idx = pb->sta_idx;
        Pblock.height = pb->height;
        Pblock.ele_num = pb->ele_num;
        Pblock.eles.resize(pb->ele_num);
        size_t data_sz = sizeof(float) * (Pblock.ele_num);
        sockBuf = (char*)malloc(data_sz);
        cur_len = 0;
        ret = 0;
        printf("Pblock header ok ele_num=%d\n", Pblock.ele_num );
        while (cur_len < data_sz)
        {
            //printf("recving 2\n");
            ret = recv(connfd, sockBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }
        //printf("check 5\n");
        float* data_eles = (float*)(void*)sockBuf;
        for (int i = 0; i < Pblock.ele_num; i++)
        {
            Pblock.eles[i] = data_eles[i];
        }
        free(data_eles);

        expected_len = sizeof(Pblock);
        sockBuf = (char*)malloc(expected_len);
        cur_len = 0;
        ret = 0;
        while (cur_len < expected_len)
        {
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }
        struct Block* qb = (struct Block*)(void*)sockBuf;
        Qblock.block_id = qb->block_id;
        Qblock.data_age = qb->data_age;
        Qblock.sta_idx = qb->sta_idx;
        Qblock.height = qb->height;
        Qblock.ele_num = qb-> ele_num;
        Qblock.eles.resize(qb->ele_num);
        printf("recv pele %d qele %d\n", Pblock.ele_num, Qblock.ele_num );
        free(sockBuf);

        data_sz = sizeof(float) * (Qblock.ele_num);
        sockBuf = (char*)malloc(data_sz);
        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            ret = recv(connfd, sockBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }

        data_eles = (float*)(void*)sockBuf;
        for (int i = 0; i < Qblock.ele_num; i++)
        {
            Qblock.eles[i] = data_eles[i];
        }
        free(data_eles);

        gettimeofday(&et, 0);
        long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
        printf("recv two blocks time = %lld\n", mksp);

        hasRecved = true;
    }
}
