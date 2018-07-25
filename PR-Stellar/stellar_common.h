
#ifndef STELLA_COMMON_H
#define STELLA_COMMON_H
//ini_rmse 46.41
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
#include <queue>
#include <list>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <atomic>
#include <fstream>
#include <sys/time.h>
#include <map>
#include <set>
#include <functional>
using namespace std;



#define N 1000990
#define M 624961
#define K  100 //主题个数
#define ROW_PS 64
#define COL_RS 64
#define ROW_UNIT (N/ROW_PS +1)
#define COL_UNIT (M/COL_RS +1)

#define PG_NUM (27665730+100)
#define CAP 500
#define ThreshIter 1000
#define WORKER_THREAD_NUM 4

#define LOG_FILE "./rmse_log"

//#define TEST_BED

#ifndef TEST_BED
#define FILE_NAME "/home/shuai/oneword/PRDS/PRData-"
#define PS_FILE "/home/shuai/oneword/out.trackers"
#define TEST_NAME "/home/shuai/oneword/validationDS"
#endif

#ifdef TEST_BED
#define FILE_NAME "/home/oneword/out.trackers"
#define TEST_NAME "/home/oneword/validationDS"
#define PS_FILE "/home/oneword/out.trackers"
#endif

#define STELLAR // ini_rmse 46.2740
#define BSP_MODE //ini_rmse 46.41
//#define ASP_MODE //ini_rmse 46.4143   120-110  200-230
//#define SSP_MODE
//#define SSP_BOUND (0) //ini_rmse  46.414
//#define SSP_BOUND (10000) //ini_rmse  46.414
//#define SSP_BOUND (1) //ini_rmse  46.383
//#define SSP_BOUND (2) //ini_rmse 46.414349
//#define SSP_BOUND (3) //ini_rmse 46.40423
struct Block
{
	int block_id;
	int data_age;
	int sta_idx;
	int height; //height
	int ele_num;
	bool isP;
	vector<float> eles;
	Block()
	{

	}
	Block operator=(Block& bitem)
	{
		block_id = bitem.block_id;
		data_age = bitem.data_age;
		height = bitem.height;
		eles = bitem.eles;
		ele_num = bitem.ele_num;
		sta_idx = bitem.sta_idx;
		return *this;
	}
	void printBlock()
	{

		printf("block_id  %d\n", block_id);
		printf("data_age  %d\n", data_age);
		printf("ele_num  %d\n", ele_num);
		for (size_t i = 0; i < eles.size(); i++)
		{
			printf("%f\t", eles[i]);
		}
		printf("\n");

	}
};

struct Entry
{
	int user_id;
	int movie_id;
	float rate;
	Entry(int uid, int mid, float rt)
	{
		user_id = uid;
		movie_id = mid;
		rate = rt;
	}
	Entry operator=(Entry& eitem)
	{
		user_id = eitem.user_id;
		movie_id = eitem.movie_id;
		rate = eitem.rate;
		return *this;
	}
};

struct ReqMsg
{
	//int block_id;
	int required_iteration;
	int worker_id;
	//I am worker id, I ask for my required(dependent) parameter at Iteration data age (i.e. the data age of the parameter should be no less than data-age)
};

struct PriorityE
{
	int worker_id;
	int block_id;
	float prior;
	int required_data_age;
	PriorityE()
	{
		worker_id = -1;
		block_id = -1;
		prior = -1;
		required_data_age = -1;
	}
	PriorityE(int wid, int bid, float p, int ra)
	{
		worker_id = wid;
		block_id = bid;
		prior = p;
		required_data_age = ra;
	}
	PriorityE operator=(const PriorityE& pitem)
	{
		worker_id = pitem.worker_id;
		block_id = pitem.block_id; //dependent blk
		prior = pitem.prior;
		required_data_age = pitem.required_data_age;
		return *this;
	}
	bool operator < (const PriorityE &a) const
	{
		return prior < a.prior; //最大值优先
	}

};

struct PageRankNode
{
	float score;
	float previous_score;
	std::vector<int> to_adj_nodes;
	std::vector<int> from_adj_nodes;
	int data_age;
	PageRankNode()
	{
		previous_score = 999;
		score = 0;
		data_age = -1;
	}
	PageRankNode(float scr, int da)
	{
		score = scr;
		data_age = da;
	}
	PageRankNode operator=(const PageRankNode& pitem)
	{
		previous_score = pitem.score;
		score = pitem.score;
		to_adj_nodes = pitem.to_adj_nodes;
		from_adj_nodes = pitem.from_adj_nodes;
		data_age = pitem.data_age;
		return *this;
	}

};

struct PNBlock
{
	int entry_num;
	int data_age;
	PNBlock()
	{

	}
	PNBlock( int en, int da)
	{
		entry_num = en;
		data_age = da;
	}

};
#endif