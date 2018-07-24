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
int main()
{
	ifstream ifs(FILE_NAME);
	if (!ifs.is_open())
	{
		printf("fail-LoadD4 to open %s\n", FILE_NAME );
		exit(-1);
	}
	int line_cnt = 0;
	while (!ifs.eof())
	{
		ifs >> from_node >> to_node;

		if (line_cnt % 1000000 == 0)
		{
			printf("Loading...%d\n", line_cnt );
		}
	}
}