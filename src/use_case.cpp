/*------------------------------------------------------------------\
|-> File Name:     use_case.cpp
|-> Author:        NL-KB
|-> Created Time:  2022.5.16
|-> Description:   KV引擎的简单用例
\------------------------------------------------------------------*/
#define CONSOLE
#include "kvengine.hpp"

#define MAX_LEVEL 6

int main()
{
	std::cout << "\n**************** Test: insert ****************\n";
	KV::KVEngine<int, std::string> kv(MAX_LEVEL);
	kv.insert(2, "not looking for");
	kv.insert(6, "a story");
	kv.insert(8, "who they are.");
	kv.insert(1, "They're");
	kv.insert(7, "that tells");

	int key = 2;
	while (kv.insert(key, "this is a duplicate key!"))
		key++;

	std::cout << "|-> data size:" << kv.size() << std::endl;

	std::cout << "\n**************** Test: dump ****************\n";
	kv.dump();

	std::cout << "\n**************** Test: load ****************\n";
	kv.load();

	std::cout << "\n**************** Test: search ****************\n";
	kv.find(2);
	kv.find(6);
	kv.find(10);

	std::cout << "\n**************** Test: print ****************\n";
	kv.print();

	std::cout << "\n**************** Test: delete ****************\n";
	kv.del(6);
	kv.del(8);
	kv.del(12);
	std::cout << "|-> data size:" << kv.size() << std::endl;
	kv.print();
}
