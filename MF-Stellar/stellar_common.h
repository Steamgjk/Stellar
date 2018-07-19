
#ifndef STELLA_COMMON_H
#define STELLA_COMMON_H

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
#include <atomic>
#include <fstream>
#include <sys/time.h>
#include <map>
using namespace std;

#define FILE_NAME "/home/shuai/oneword/trainDS/"
#define TEST_NAME "/home/shuai/oneword/validationDS"
#define N 1000990
#define M 624961
#define K  100 //主题个数
#define ROW_PS 64
#define COL_RS 64
#define ROW_UNIT (N/ROW_PS +1)
#define COL_UNIT (M/COL_RS +1)

#define CAP 500
#define ThreshIter 1000
#define WORKER_THREAD_NUM 4

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

#endif