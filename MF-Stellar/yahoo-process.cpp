#include <string>
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
#include <queue>
#include <map>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <atomic>
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <queue>
using namespace std;
#define N 1000990
#define M 624961
#define ROW_PS 64
#define COL_RS 64
#define ROW_UNIT (N/ROW_PS +1)
#define COL_UNIT (M/COL_RS +1)
#define TrainF "./trainIdx1.txt"
#define TestF "./testIdx1.txt"
std::map<long, double> Rmap;
std::map<long, double> RTest;
std::map<long, double> OutPutTrain[ROW_PS][COL_RS];
//max_user=1000989 min_user=0
//#define OutTrainF "./yahoo-output/train-"
#define OutTrainF "./trainDS/"
#define OutTestF "./testDS"

void writeFile(int idx)
{
	char fn[100];
	for (int j = 0; j < COL_RS; j++)
	{
		sprintf(fn, "%s%d-%d", OutTrainF, idx, j);
		ofstream ofs(fn, ios::trunc);
		std::map<long, double>::iterator iter = OutPutTrain[idx][j].begin();
		int cnt = 0;
		for (iter; iter != OutPutTrain[idx][j].end(); iter++)
		{
			long hash_id = iter->first;
			double rate = iter->second;
			long user_id = hash_id / M;
			long movie_id = hash_id % M;
			ofs << user_id << "\t" << movie_id << "\t" << rate << endl;
			cnt++;
			if (cnt % 100000 == 0)
			{
				printf("[%d]:%d\n", idx, cnt );
			}
		}
	}

	printf("Finish-[%d]\n", idx);
}
int main()
{


	ifstream ifstest(TestF, ios::in | ios::out);
	ofstream ofstest(OutTestF, ios::trunc);

	ifstream ifs(TrainF, ios::in | ios::out);
	//ofstream ofstrain(OutTrainF, ios::trunc);

	long user_id;
	long movie_id;
	double rate;
	char tmp;
	string line;
	int cnt = 0;
	int anum;
	long row, col;
	//long row_unit = N / DIM_SZ;
	//long col_unit = M / DIM_SZ;
	string tmpstr;
	for (int i = 0; i < ROW_PS; i++)
	{
		for (int j = 0; j < COL_RS; j++)
		{
			OutPutTrain[i][j].clear();
		}

	}
	int test_cnt = 0;


	while (!ifstest.eof())
	{
		ifstest >> user_id >> tmp >> anum;
		for (int i = 0; i < anum; i++)
		{
			ifstest >> movie_id >> rate >> tmpstr >> tmpstr;
			long hash_id = user_id * M + movie_id;
			ofstest << user_id << "\t" << movie_id << "\t" << rate << endl;
			test_cnt++;
			if (test_cnt % 10000 == 0)
			{
				printf("test_cnt=%d\n", test_cnt );
			}
		}
	}
	printf("test fini\n");
	exit(0);


	/*
		test_cnt = 0;
		long long max_user = -1;
		long long min_user = 0xffffff;
		while (!ifs.eof())
		{
			ifs >> user_id >> tmp >> anum;
			for (int i = 0; i < anum; i++)
			{
				ifs >> movie_id >> rate >> tmpstr >> tmpstr;
				long hash_id = user_id * M + movie_id;
				OutPutTrain[user_id / ROW_UNIT][movie_id / COL_UNIT].insert(pair<long, double>(hash_id, rate));
				test_cnt++;
				if (test_cnt % 1000000 == 0)
				{
					printf("test_cnt=%d\n", test_cnt);
				}
			}

		}
		//printf("max_user=%lld min_user=%lld\n", max_user, min_user );
		for (int i = 0; i < ROW_PS; i++)
		{
			for (int j = 0; j < COL_RS; j++)
			{
				printf("[%d][%d]-[%ld]\n", i, j, OutPutTrain[i][j].size());
			}
		}
		getchar();
		**/
	/*
	while (!ifs.eof())
	{
		ifs >> user_id >> tmp >> anum;
		row = user_id / row_unit;
		for (int i = 0; i < anum; i++)
		{
			ifs >> movie_id >> rate >> tmpstr >> tmpstr;
			col = movie_id / col_unit;
			if (row >=  DIM_SZ)
			{
				row = DIM_SZ - 1;
			}
			if (col >= DIM_SZ)
			{
				col = DIM_SZ - 1;
			}
			long hash_id = user_id * M + movie_id;
			OutPutTrain[row * DIM_SZ + col].insert(pair<long, double>(hash_id, rate));
			test_cnt++;
			if (test_cnt % 100000 == 0)
			{
				printf("test_cnt=%d\n", test_cnt );
			}
		}

	}


	**/
	for (int i = 0; i < ROW_PS; i++)
	{
		std::thread WriteTd(writeFile, i);
		WriteTd.detach();
	}


	while (1 == 1)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	}


}