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
#define FILE_NAME "/home/shuai/oneword/out.trackers"
#define PG_NUM (27665730+100)
#define WORKER_NUM 16
int num_lens[100];
std::vector<pair<int, int> > vec[100];


void writeFile(int worker_id)
{
	char fn[100];
	sprintf(fn, "/home/shuai/oneword/PRDS/PRData-%d", worker_id);
	ofstream ofs(fn, ios::trunc);
	int cnt = 0;
	for (int i = 0; i < vec[worker_id].size(); i++)
	{
		ofs << vec[worker_id][i].first << "\t" << vec[worker_id][i].second << endl;
		cnt++;
		if (cnt % 100000 == 0)
		{
			printf("[%d] write %d\n", worker_id, cnt );
		}
	}
	printf("Finish-[%d]\n", worker_id);
}

int main()
{
	ifstream ifs(FILE_NAME);
	if (!ifs.is_open())
	{
		printf("fail-LoadD4 to open %s\n", FILE_NAME );
		exit(-1);
	}
	int line_cnt = 0;
	int from_node, to_node;
	string str;
	int block_units = PG_NUM / WORKER_NUM;
	for (int i = 0; i < WORKER_NUM; i++)
	{
		num_lens[i] = i * block_units;
	}
	num_lens[WORKER_NUM] = PG_NUM;

	getline(ifs, str);
	while (!ifs.eof())
	{

		ifs >> from_node >> to_node;

		for (int i = 0; i < WORKER_NUM; i++)
		{
			if ( (from_node >= num_lens[i] && from_node < num_lens[i + 1]) || (to_node >= num_lens[i] && to_node < num_lens[i + 1]) )
			{
				vec[i].push_back(pair<int, int>(from_node, to_node));
			}
		}

		line_cnt++;
		if (line_cnt % 1000000 == 0)
		{
			printf("Loading...%d  %d  %d\n", line_cnt, from_node, to_node );
		}
	}
	printf("ok begin write\n");
	for (int i = 0; i < WORKER_NUM; i++)
	{
		std::thread WriteTd(writeFile, i);
		WriteTd.detach();
	}


	while (1 == 1)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	}
}