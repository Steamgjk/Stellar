//
//  main.cpp
//  linux_socket_api
//
//  Created by Jinkun Geng on 18/05/11.
//  Copyright (c) 2016年 Jinkun Geng. All rights reserved.
//
#include "stellar_common.h"
using namespace std;

/*
#define FILE_NAME "./mdata/traina-"
#define TEST_NAME "./mdata/testa-"
#define N 71567
#define M 65133
#define K  40 //主题个数
**/

/*Jumbo **/
/*
#define FILE_NAME "./data/TrainingMap-"
#define TEST_NAME "./data/TestMap-"
#define N 1000000
#define M 1000000
#define K  100 //主题个数
**/
/**Yahoo!Music **/



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
int worker_pidx[CAP];
int worker_qidx[CAP];
int row_lens[20];
int col_lens[20];
int row_unit = ROW_UNIT * (ROW_PS / WORKER_NUM);
int col_unit = COL_UNIT * (COL_RS / WORKER_NUM);
long long time_span[300];
std::vector<int> vec_uids;
std::vector<int> vec_mids;
std::vector<float> vec_rates;
bool sendConnected[100];
bool recvConnected[100];

int iter_t = 0;
int iter_thresh = 10;

int main(int argc, const char * argv[])
{
    ofstream ofs(LOG_FILE, ios::trunc);
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
    //gen P and Q
    if (argc == 2)
    {
        WORKER_NUM = atoi(argv[1]) ;
    }
    LoadTestRating();

    for (int recv_thread_id = 0; recv_thread_id < WORKER_NUM; recv_thread_id++)
    {
        recvConnected[recv_thread_id] = false;
        int thid = recv_thread_id;
        printf("thid=%d\n", thid );
        std::thread recv_thread(recvTd, thid);
        recv_thread.detach();
    }
    for (int send_thread_id = 0; send_thread_id < WORKER_NUM; send_thread_id++)
    {
        sendConnected[send_thread_id] = false;
        int thid = send_thread_id;
        std::thread send_thread(sendTd, thid);
        send_thread.detach();
    }
    /*
        while (1 == 1)
        {
            bool ok = true;
            for (int i = 0; i < WORKER_NUM; i++)
            {
                if (sendConnected[i] == false)
                {
                    printf("%d send not connected\n", i );
                    ok = false;
                }
                if (recvConnected[i] == false)
                {
                    printf("%d recv not connected\n", i );
                    ok = false;
                }
            }
            if (ok)
            {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(3000));
        }
    **/
    srand(1);



    row_unit = ROW_UNIT * (ROW_PS / WORKER_NUM);
    col_unit = COL_UNIT * (COL_RS / WORKER_NUM);
    printf("row_unit = %d col_unit=%d\n", row_unit, col_unit );
    for (int i = 0; i < WORKER_NUM; i++)
    {
        row_lens[i] = i * row_unit;
        col_lens[i] = i * col_unit;
    }
    row_lens[WORKER_NUM] = WORKER_NUM * row_unit;
    col_lens[WORKER_NUM] = WORKER_NUM * col_unit;

    printf("start work\n");
    partitionP(Pblocks);
    partitionQ(Qblocks);
    for (int i = 0; i < WORKER_NUM; i++)
    {
        printf("Psz [%d][%ld]  Qsz [%d][%ld]\n", Pblocks[i].ele_num, Pblocks[i].eles.size(), Qblocks[i].ele_num, Qblocks[i].eles.size() );
        for (int j = 0; j < Pblocks[i].ele_num; j++)
        {
            Pblocks[i].eles[j] = drand48() * 0.2;
        }
        for (int j = 0; j < Qblocks[i].ele_num; j++)
        {
            Qblocks[i].eles[j] = drand48() * 0.2;
        }
    }
    float ini_rmse = CalcRMSE();
    printf("ini_rmse = %f\n", ini_rmse );

    for (int i = 0; i < WORKER_NUM; i++)
    {
        canSend[i] = false;
    }
    for (int i = 0; i < WORKER_NUM; i++)
    {
        worker_pidx[i] = worker_qidx[i] = i;
    }

    struct timeval beg, ed;
    iter_t = 0;
    while (1 == 1)
    {
        srand(time(0));
        /*
        random_shuffle(worker_qidx, worker_qidx + WORKER_NUM); //迭代器
        for (int i = 0; i < WORKER_NUM; i++)
        {
            printf("%d  [%d:%d]\n", i, worker_pidx[i], worker_qidx[i] );
        }

        printf("[%d]canSend...!\n", iter_t);
        for (int i = 0; i < WORKER_NUM; i++)
        {
            canSend[i] = true;
        }
        //getchar();

        while (recvCount != WORKER_NUM)
        {
            //cout << "RecvCount\t" << recvCount << endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        **/
        if (!curIterFin(iter_t))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        if (iter_t == 0)
        {
            gettimeofday(&beg, 0);
        }
        printf("RMSE-thd iter_t=%d\n", iter_t );
        //if (recvCount == WORKER_NUM)
        {
            //if (iter_t % 10 == 0 )
            if (iter_t == iter_thresh)
            {
                waitfor = true;
                printf("Entering statistics...\n");
                gettimeofday(&ed, 0);
                time_span[iter_t / 10] = (ed.tv_sec - beg.tv_sec) * 1000000 + ed.tv_usec - beg.tv_usec;
                float pmin, pmax, qmin, qmax;
                pmin = 9999999;
                qmin = 9999999;
                pmax = -1;
                qmax = -1;
                for (int ii = 0; ii < WORKER_NUM; ii++)
                {
                    //WriteLog(Pblocks[ii], Qblocks[ii], iter_t);
                    for (int jj = 0; jj < Pblocks[ii].ele_num; jj++)
                    {
                        if (pmin > fabs(Pblocks[ii].eles[jj]))
                        {
                            pmin = fabs(Pblocks[ii].eles[jj]);
                        }
                        if (pmax < fabs(Pblocks[ii].eles[jj]))
                        {
                            pmax = fabs(Pblocks[ii].eles[jj]);
                        }

                    }
                    for (int jj = 0; jj < Qblocks[ii].ele_num; jj++)
                    {
                        if (qmin > fabs(Qblocks[ii].eles[jj]))
                        {
                            qmin = fabs(Qblocks[ii].eles[jj]);
                        }
                        if (qmax < fabs(Qblocks[ii].eles[jj]))
                        {
                            qmax = fabs(Qblocks[ii].eles[jj]);
                        }

                    }
                    printf("iter=%d pmin=%f pmax=%f qmin=%f qmax=%f\n", iter_t, pmin, pmax, qmin, qmax );
                    recvCount = 0;
                }
                printf("Calclating RMSE... \n");
                float rmse = CalcRMSE();
                //ofs << iter_t << "\t" << rmse << endl;
                printf("time= %d\t%lld rmse=%f\n", iter_t, time_span[iter_t / 10], rmse );
                exit(0);

            }

        }
        iter_t++;
        if (iter_t % 100 == 0)
        {
            for (int i = 0; i <= iter_t / 10; i++)
            {
                printf("%lld\n", time_span[i] );
            }

        }

    }
    return 0;
}
bool curIterFin(int curIter)
{
    if (curIter < 0)
    {
        return true;
    }
    for (int i = 0; i < WORKER_NUM; i++)
    {
        if (Pblocks[i].data_age <= curIter || Qblocks[i].data_age <= curIter)
        {
            return false;
        }
    }
    return true;
}
void LoadTestRating()
{
    vec_mids.clear();
    vec_uids.clear();
    vec_rates.clear();
    ifstream ifs(TEST_NAME, ios::in | ios::out);
    int user_id, movie_id;
    float rate;
    while (!ifs.eof())
    {
        ifs >> user_id >> movie_id >> rate;
        vec_uids.push_back(user_id);
        vec_mids.push_back(movie_id);
        vec_rates.push_back(rate);
        /*
        test_cnt++;
        if (test_cnt % 10000 == 0)
        {
            printf("test_cnt=%d\n",  test_cnt);
        }
        **/
    }
}
float CalcRMSE()
{
    float rmse = 0;
    size_t cnt = 0;
    size_t ele_num = vec_rates.size();
    int user_id, movie_id;
    float rate;

    for (cnt = 0; cnt < ele_num; cnt++)
    {
        user_id = vec_uids[cnt];
        movie_id = vec_mids[cnt];
        rate = vec_rates[cnt];
        int pblock_idx = user_id / row_unit;
        int qblock_idx = movie_id / col_unit;
        int p_ini_idx = user_id - (pblock_idx * row_unit);
        int q_ini_idx = movie_id - (qblock_idx * col_unit);
        float sum = 0;
        for (int i = 0; i < K; i++)
        {
            sum += (Pblocks[pblock_idx].eles[p_ini_idx * K + i] * 10) * (Qblocks[qblock_idx].eles[q_ini_idx * K + i] * 10);
        }
        rmse += (rate - sum) * (rate - sum);
    }
    rmse = sqrt(rmse / cnt);
    //printf("rmse= %f\n", rmse);
    return rmse;
}
void WriteLog(Block & Pb, Block & Qb, int iter_cnt)
{
    char fn[100];
    sprintf(fn, "./Rtrack/Pblock-%d-%d", iter_cnt, Pb.block_id);
    ofstream pofs(fn, ios::trunc);
    for (int h = 0; h < Pb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            pofs << Pb.eles[h * K + j] << " ";
        }
        pofs << endl;
    }
    printf("fn:%s\n", fn );
    sprintf(fn, "./Rtrack/Qblock-%d-%d", iter_cnt, Qb.block_id);
    ofstream qofs(fn, ios::trunc);
    for (int h = 0; h < Qb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            qofs << Qb.eles[h * K + j] << " ";
        }
        qofs << endl;
    }
    printf("fn:%s\n", fn );
}
bool isReady(int block_id, int required_iter, int fd)
{
    size_t struct_sz = sizeof(Block);
    size_t data_sz = 0;
    char* buf = NULL;
    bool ready = false;

    //for BSP constraints
#ifdef BSP_MODE
    if (!curIterFin(required_iter - 1))
    {
        //printf("%d iter cannot start\n", data_age );
        return false;
    }

    if (iter_t < required_iter)
    {
        return false;
    }
#endif

    /*
    else
    {
        printf("%d iter can start\n", data_age );
    }
    **/
    //getchar();
    if (block_id < WORKER_NUM)
    {
        // is P block
#ifdef BSP_MODE
        if (Pblocks[block_id].data_age >= required_iter)
#endif
        {
            //is P block
            int pbid = block_id;
            data_sz = sizeof(float) * Pblocks[pbid].eles.size();
            buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Pblocks[pbid]), struct_sz);
            memcpy(buf + struct_sz, (char*) & (Pblocks[pbid].eles[0]), data_sz);
            ready = true;
        }
#ifdef BSP_MODE
        else
        {
            return false;
        }
#endif
    }
    else
    {
        // is Q block
        block_id -= WORKER_NUM;
        //printf("Q real blockid =%d age1=%d age2=%d\n", block_id, Qblocks[block_id].data_age, data_age  );
#ifdef BSP_MODE
        if (Qblocks[block_id].data_age >= required_iter)
#endif
        {
            int qbid = block_id;
            data_sz = sizeof(float) * Qblocks[qbid].eles.size();
            buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Qblocks[qbid]), struct_sz);
            memcpy(buf + struct_sz , (char*) & (Qblocks[qbid].eles[0]), data_sz);
            ready = true;
        }
#ifdef BSP_MODE
        else
        {
            return false;
        }
#endif
    }

    size_t to_send_len = 4096;
    size_t remain_len = struct_sz + data_sz;
    size_t sent_len = 0;
    int ret = -1;
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

    return ready;
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
    return fd;
}
//send only establish the fd vec, send in sequence

//recving request and then decide can/cannot sent
//corresponding to pull in worker
void sendTd(int send_thread_id)
{
    int fd = genActivePushfd(send_thread_id);
    sendConnected[send_thread_id] = true;

    ReqMsg* msg = (ReqMsg*)malloc(sizeof(ReqMsg));
    int ret = -1;
    while (1 == 1)
    {
        if (waitfor)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        ret = recv(fd, msg, sizeof(ReqMsg), 0);
        int required_pid = msg->worker_id;
        int required_qid = (msg->worker_id + msg->required_iteration) % WORKER_NUM + WORKER_NUM;
        printf("[%d]it is asking for %d iter and pid=%d qid=%d\n", send_thread_id, msg->required_iteration, required_pid, required_qid );
        while (1 == 1)
        {
            if (isReady(required_pid, msg->required_iteration, fd))
            {
                break;
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        printf("[%d] iter=%d send to worker [%d] pid=%d\n", send_thread_id, msg->required_iteration, msg->worker_id, required_pid  );
        while (1 == 1)
        {
            if (isReady(required_qid, msg->required_iteration, fd))
            {
                break;
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        //printf("[%d] iter=%d send to worker [%d] qid=%d\n", send_thread_id, msg->required_iteration, msg->worker_id, required_qid  );
        //canSend[send_thread_id] = false;

    }

}



void recvTd(int recv_thread_id)
{
    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );
    recvConnected[recv_thread_id] = true;
    bool one_p = false;
    bool one_q = false;
    while (1 == 1)
    {
        if (waitfor)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        //printf("recving ...\n");
        struct timeval st, et;
        gettimeofday(&st, 0);
        size_t expected_len = sizeof(Block);
        char* sockBuf = (char*)malloc(expected_len);
        size_t cur_len = 0;
        int ret = 0;
        while (cur_len < expected_len)
        {

            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret <=  0)
            {
                printf("[%d] Mimatch! %d\n", recv_thread_id, ret);
                if (ret == 0)
                {
                    exit(-1);
                }
            }
            //printf("ret=%d\n", ret );
            cur_len += ret;
            //printf("cur_len=%d expected_len=%d\n", cur_len, expected_len );
        }
        struct Block* pb = (struct Block*)(void*)sockBuf;
        //pb->printBlock();
        size_t data_sz = sizeof(float) * (pb->ele_num);
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

        float* data_eles = (float*)(void*)dataBuf;
        int block_idx = pb->block_id ;
        if (block_idx < WORKER_NUM)
        {
            //is Pblock
            Pblocks[block_idx].block_id = pb->block_id;
            Pblocks[block_idx].sta_idx = pb->sta_idx;
            Pblocks[block_idx].height = pb->height;
            Pblocks[block_idx].ele_num = pb->ele_num;
            Pblocks[block_idx].eles.resize(pb->ele_num);
            Pblocks[block_idx].isP = pb->isP;
            for (int i = 0; i < pb->ele_num; i++)
            {
                //Pblocks[block_idx].eles[i] += data_eles[i];
                Pblocks[block_idx].eles[i] = data_eles[i];
            }
            Pblocks[block_idx].data_age++;
            one_p = true;
        }
        else
        {
            // is Qblock
            block_idx -= WORKER_NUM;
            Qblocks[block_idx].block_id = pb->block_id;
            Qblocks[block_idx].sta_idx = pb->sta_idx;
            Qblocks[block_idx].height = pb->height;
            Qblocks[block_idx].ele_num = pb->ele_num;
            Qblocks[block_idx].eles.resize(pb->ele_num);
            Qblocks[block_idx].isP = pb->isP;
            for (int i = 0; i < pb->ele_num; i++)
            {
                //Qblocks[block_idx].eles[i] += data_eles[i];
                Qblocks[block_idx].eles[i] = data_eles[i];
            }
            Qblocks[block_idx].data_age++;
            one_q = true;
        }

        free(sockBuf);
        free(dataBuf);

        if (one_p && one_q)
        {
            one_p = false;
            one_q = false;
            recvCount++;
        }
        gettimeofday(&et, 0);
        long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
        //printf("[%d]recv success time = %lld\n", recv_thread_id, mksp );

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
    printf("get connection from %s  %d\n", inet_ntoa(addressClient.sin_addr), addressClient.sin_port);
    return connfd;

}



void partitionP(Block * Pblocks)
{
    int i = 0;
    int height = row_unit;
    int last_height = N - (WORKER_NUM - 1) * height;
    printf("P height=%d  last_height=%d\n", height, last_height );
    for (i = 0; i < WORKER_NUM; i++)
    {
        Pblocks[i].block_id = i;
        Pblocks[i].data_age = 0;
        Pblocks[i].eles.clear();
        Pblocks[i].height = height;
        if ( i == WORKER_NUM - 1)
        {
            Pblocks[i].height = last_height;
        }
        Pblocks[i].sta_idx = row_lens[i];
        Pblocks[i].ele_num = Pblocks[i].height * K;
        Pblocks[i].eles.resize(Pblocks[i].ele_num);
    }

}

void partitionQ(Block * Qblocks)
{
    int i = 0;
    int height = col_unit;
    int last_height = M - (WORKER_NUM - 1) * height;
    printf("Q height=%d  last_height=%d\n", height, last_height );
    for (i = 0; i < WORKER_NUM; i++)
    {
        Qblocks[i].block_id = i + WORKER_NUM;
        Qblocks[i].data_age = 0;
        Qblocks[i].eles.clear();
        Qblocks[i].height = height;
        if ( i == WORKER_NUM - 1)
        {
            Qblocks[i].height = last_height;
        }
        Qblocks[i].sta_idx = col_lens[i];
        Qblocks[i].ele_num = Qblocks[i].height * K;
        Qblocks[i].eles.resize(Qblocks[i].ele_num);

    }

}




//////////////////////////////////

void sendTd1(int send_thread_id)
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
    assert(check_ret >= 0);
    printf("[Td:%d]connected %s  %d\n", send_thread_id, remote_ip, remote_port );
    while (1 == 1)
    {
        if (canSend[send_thread_id])
        {
            int pbid = worker_pidx[send_thread_id];
            int qbid = worker_qidx[send_thread_id];
            size_t struct_sz = sizeof( Pblocks[pbid]);
            size_t data_sz = sizeof(float) * Pblocks[pbid].eles.size();
            char* buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Pblocks[pbid]), struct_sz);
            memcpy(buf + struct_sz, (char*) & (Pblocks[pbid].eles[0]), data_sz);
            size_t to_send_len = 4096;
            size_t remain_len = struct_sz + data_sz;
            size_t sent_len = 0;
            int ret = -1;
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

            struct_sz = sizeof( Qblocks[qbid]);
            data_sz = sizeof(float) * Qblocks[qbid].eles.size();
            buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Qblocks[qbid]), struct_sz);
            memcpy(buf + struct_sz , (char*) & (Qblocks[qbid].eles[0]), data_sz);
            remain_len = struct_sz + data_sz;
            sent_len = 0;
            ret = -1;
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
            canSend[send_thread_id] = false;
        }
    }

}

void recvTd1(int recv_thread_id)
{
    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );
    while (1 == 1)
    {
        //printf("recving ...\n");
        struct timeval st, et;
        gettimeofday(&st, 0);
        size_t expected_len = sizeof(Block);
        char* sockBuf = (char*)malloc(expected_len);
        size_t cur_len = 0;
        int ret = 0;
        while (cur_len < expected_len)
        {
            //printf("[Td:%d] cur_len = %ld expected_len-cur_len = %ld\n", recv_thread_id, cur_len, expected_len - cur_len );
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret <=  0)
            {
                printf("Mimatch! %d\n", ret);
                if (ret == 0)
                {
                    exit(-1);
                }
            }
            //printf("ret=%d\n", ret );
            cur_len += ret;
            //printf("cur_len=%d expected_len=%d\n", cur_len, expected_len );
        }
        //printf("come here\n");
        struct Block* pb = (struct Block*)(void*)sockBuf;
        //pb->printBlock();
        size_t data_sz = sizeof(float) * (pb->ele_num);
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

        float* data_eles = (float*)(void*)dataBuf;
        int block_idx = pb->block_id ;
        Pblocks[block_idx].block_id = pb->block_id;
        Pblocks[block_idx].sta_idx = pb->sta_idx;
        Pblocks[block_idx].height = pb->height;
        Pblocks[block_idx].ele_num = pb->ele_num;
        Pblocks[block_idx].eles.resize(pb->ele_num);
        Pblocks[block_idx].isP = pb->isP;
        for (int i = 0; i < pb->ele_num; i++)
        {
            Pblocks[block_idx].eles[i] = data_eles[i];
        }
        free(sockBuf);
        free(dataBuf);

        //printf("successful rece one Block data_sz = %ld block_sz=%ld\n", data_sz, expected_len);
        expected_len = sizeof(Block);
        sockBuf = (char*)malloc(expected_len);
        cur_len = 0;
        ret = 0;
        while (cur_len < expected_len)
        {
            //printf("[Td:%d] cur_len = %ld expected_len-cur_len = %ld\n", recv_thread_id, cur_len, expected_len - cur_len );
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret <=  0)
            {
                printf("Mimatch! %d\n", ret);
                if (ret == 0)
                {
                    exit(-1);
                }
            }
            cur_len += ret;
        }
        pb = (struct Block*)(void*)sockBuf;
        data_sz = sizeof(float) * (pb->ele_num);
        dataBuf = (char*)malloc(data_sz);
        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            ret = recv(connfd, dataBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }

        data_eles = (float*)(void*)dataBuf;
        block_idx = pb->block_id ;
        Qblocks[block_idx].block_id = pb->block_id;
        Qblocks[block_idx].sta_idx = pb->sta_idx;
        Qblocks[block_idx].height = pb->height;
        Qblocks[block_idx].ele_num = pb->ele_num;
        Qblocks[block_idx].eles.resize(pb->ele_num);
        Qblocks[block_idx].isP = pb->isP;
        for (int i = 0; i < pb->ele_num; i++)
        {
            Qblocks[block_idx].eles[i] = data_eles[i];
        }

        //printf("[]successful rece another Block\n");
        free(sockBuf);
        free(dataBuf);
        gettimeofday(&et, 0);
        long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
        printf("[%d]recv success time = %lld\n", recv_thread_id, mksp );
        recvCount++;
    }
}