#include "stellar_common.h"

int WORKER_NUM = 16;
int row_lens[20];
int col_lens[20];
int row_unit = ROW_UNIT * (ROW_PS / WORKER_NUM);
int col_unit = COL_UNIT * (COL_RS / WORKER_NUM);

using namespace std;


int check_points[40] = {2, 7, 13, 18, 24, 30, 36, 41, 47, 53, 60, 65, 70, 75, 82, 87, 93, 98, 104, 110, 116, 122, 127, 133, 138, 144, 149, 156, 162, 167, 173};

std::vector<int> vec_uids;
std::vector<int> vec_mids;
std::vector<float> vec_rates;

Block Pblocks[30];
Block Qblocks[30];


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

    //LoadTestRating();
    char filename[100];
    //int i = 0;
    //int i = atoi(argv[3]);
    partitionP(Pblocks);
    partitionQ(Qblocks);
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
            int num = 0;
            while (!ifs.eof())
            {
                /*
                for (int kk = 0; kk < K; kk++)
                {
                    ifs >> P[row_idx][kk];
                }
                row_idx++;
                **/
                ifs >> Pblocks[j].eles[num];
                num++;
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
            num = 0;
            while (!ifs.eof())
            {
                /*
                                for (int kk = 0; kk < K; kk++)
                                {
                                    ifs >> Q[kk][col_idx];
                                }
                                col_idx++;
                                **/
                ifs >> Qblocks[j].eles[num];
                num++;
            }
            ifs.close();
            //printf("%s read\n", filename );

        }

        rmse = 0;
        int cnt = 0;
        int user_id, movie_id;
        float rate;

        ifstream ifs(TEST_NAME, ios::in | ios::out);
        int test_cnt = 0;
        while (!ifs.eof())
        {
            ifs >> user_id >> movie_id >> rate;
            test_cnt++;
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
            /*


                        if (test_cnt % 10000 == 0)
                        {
                            printf("test_cnt=%d\n",  test_cnt);
                        }

                        float sum = 0;
                        for (int ii = 0; ii < K; ii++)
                        {
                            sum += (P[user_id][ii] * 10) * (Q[ii][movie_id] * 10);
                        }
                        //printf("user_id=%d movie_id=%d rate=%f sum=%f\n", user_id, movie_id, rate, sum  );
                        //getchar();
                        rmse += (rate - sum) * (rate - sum);
            **/

        }


        rmse /= test_cnt;
        rmse = sqrt(rmse);

        printf("iter=%d rmse=%lf\n", i, rmse);
        ofs << i << "\t" << rmse << endl;

    }
    return 0;
}
