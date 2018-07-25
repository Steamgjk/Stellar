#include <iostream>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <cmath>
#include <time.h>
#include <vector>
#include <list>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <map>


#define FILE_NAME "./yahoo-output/train-"
#define TEST_NAME "/home/shuai/oneword/validationDS"

#define N 1000990
#define M 624961
#define K  100 //主题个数


int ITER_NUM  = 2000;
int PORTION_NUM = 4;
double P[N][K];
double Q[K][M];
int WORKER_NUM = 16;
using namespace std;


int check_points[40] = {2, 7, 13, 18, 24, 30, 36, 41, 47, 53, 60, 65, 70, 75, 82, 87, 93, 98, 104, 110, 116, 122, 127, 133, 138, 144, 149, 156, 162, 167, 173};

std::vector<int> vec_uids;
std::vector<int> vec_mids;
std::vector<float> vec_rates;

void LoadTestRating()
{
    vec_mids.clear();
    vec_uids.clear();
    vec_rates.clear();
    ifstream ifs(TEST_NAME, ios::in | ios::out);
    int user_id, movie_id;
    float rate;
    int test_cnt = 0;
    while (!ifs.eof())
    {
        ifs >> user_id >> movie_id >> rate;
        vec_uids.push_back(user_id);
        vec_mids.push_back(movie_id);
        vec_rates.push_back(rate);

        test_cnt++;
        if (test_cnt % 10000 == 0)
        {
            printf("test_cnt=%d\n",  test_cnt);
        }

    }
    printf("ele=%d\n", vec_rates.size());
    getchar();
}

int main(int argc, const char * argv[])
{
    ofstream ofs("./rima.txt", ios::trunc);
    int stat = 0;

    ifstream ifs;
    double rmse = 0;

    LoadTestRating();
    char filename[100];
    //int i = 0;
    //int i = atoi(argv[3]);
    for (int log_idx = 0 ; log_idx < 31; log_idx++)
    {
        int i = check_points[log_idx];

        int row_idx = 0;
        int col_idx = 0;
        for (int j = 0 ; j < WORKER_NUM; j++)
        {
            sprintf(filename, "./PS-track/Pblock-%d-%d", i, j);
            ifs.open(filename, ios::in | ios::out);
            if (!ifs.is_open())
            {
                printf("fail to open %s\n", filename);
                getchar();
            }
            else
            {
                printf("open %s\n", filename );
            }
            //getchar();
            while (!ifs.eof())
            {
                for (int kk = 0; kk < K; kk++)
                {
                    ifs >> P[row_idx][kk];
                }
                row_idx++;
                //if (row_idx % 1000 == 0)
                // printf("row_idx=%d\n", row_idx);
            }
            ifs.close();
            //printf("%s read\n", filename );
            //getchar();
            sprintf(filename, "./PS-track/Qblock-%d-%d", i, j + WORKER_NUM);
            //ifstream ifs1(filename, ios::in | ios::out);
            ifs.open(filename, ios::in | ios::out);
            if (!ifs.is_open())
            {
                printf("fail to open %s\n", filename);
                getchar();
            }
            double temp;
            while (!ifs.eof())
            {

                for (int kk = 0; kk < K; kk++)
                {
                    ifs >> Q[kk][col_idx];
                }

                col_idx++;
            }
            ifs.close();
            //printf("%s read\n", filename );

        }

        rmse = 0;
        int cnt = 0;
        int ele_num = vec_rates.size();
        printf("ele_num=%d\n", ele_num );
        int user_id, movie_id;
        float rate;
        for (cnt = 0; cnt < ele_num; cnt++)
        {
            user_id = vec_uids[cnt];
            movie_id = vec_mids[cnt];
            rate = vec_rates[cnt];

            float sum = 0;
            for (int ii = 0; ii < K; ii++)
            {
                sum += (P[user_id][ii] * 10) * (Q[ii][movie_id] * 10);
            }
            printf("user_id=%d movie_id=%d rate=%f sum=%f\n", user_id, movie_id, rate, sum  );
            getchar();
            rmse += (rate - sum) * (rate - sum);
        }
        rmse /= cnt;
        rmse = sqrt(rmse);

        printf("iter=%d rmse=%lf\n", i, rmse);
        ofs << i << "\t" << rmse << endl;

    }
    return 0;
}
