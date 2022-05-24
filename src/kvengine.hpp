/*------------------------------------------------------------------\
|-> File Name:		kvengine.hpp
|-> Author:			NL-KB
|-> Created Time:	2022.5.16
|-> Description:	基于跳表的key-value存储引擎
\------------------------------------------------------------------*/
#ifndef KVENGINE
#define KVENGINE
#include <cstring>
#include <iostream>
#include <sstream>
#include <fstream>
#include <filesystem>
#include <mutex>

namespace KV
{
	const char* FILE_PATH = "save/data";

	/*------------------------------------------------------------------\
	|-> 简述:
	|		 跳表通过多层索引实现高效的查询、插入和删除。多层索引的实现
	|	主要依托于 `next` 数据成员，所以理解 `next` 是理解跳表工作原理的关键
	|	所在。`next` 是一个指向指针数组的指针，大小由 `level` 记录。它记录了
	|	该节点指向每一层的后驱的指针，当指针内容为 NULL 时意味着该节点是
	|	这一层的最后一个结点。
	|
	\------------------------------------------------------------------*/
	template <class K, class V>
	struct Node
	{
	public:
		Node() {}
		Node(K, V, int);
		~Node();

	public:
		K key;
		V value;
		Node<K, V>** next;
		int level;
	};

	template <class K, class V>
	Node<K, V>::Node(const K k, const V v, int level)
	{
		this->key = k;
		this->value = v;
		this->level = level;
		this->next = new Node<K, V> * [level + 1];
		memset(this->next, 0, sizeof(Node<K, V>*) * (level + 1));
	};

	template <class K, class V>
	Node<K, V>::~Node()
	{
		delete[] next;
	};

	/*------------------------------------------------------------------\
	|-> 简述:
	|		KVEngine 是基于跳表实现的。引擎可以在多线程并发的情况下，实现
	|	对键值对数据的高性能查询、增删。
	|
	\------------------------------------------------------------------*/
	template <class K, class V>
	class KVEngine
	{
	public:
		KVEngine(int max_evel);
		~KVEngine();

		bool insert(K, V);
		bool del(K);
		bool find(K);

		int size();
		void print();

		void dump();
		void load();

	private:
		int get_random_level();
		void split_kv_str(const std::string& str, std::string& key,
			std::string& value);
		bool is_valid(const std::string& str);

	private:
		int _max_level;
		int _current_level;
		int _size;
		Node<K, V>* _header;	// new and delete
		Node<K, V>** _tmp_next; // new and delete
		std::string _sep;
		std::ofstream _writer;
		std::ifstream _reader;
		std::mutex mtx;
	};

	template <class K, class V>
	KVEngine<K, V>::KVEngine(int max_level)
	{
		this->_max_level = max_level;
		this->_current_level = 0;
		this->_size = 0;
		this->_header = new Node<K, V>(K(), V(), _max_level);
		this->_tmp_next = new Node<K, V> * [this->_max_level + 1];
		this->_sep = ":";
	};

	template <class K, class V>
	KVEngine<K, V>::~KVEngine()
	{
		if (_writer.is_open())
		{
			_writer.close();
		}
		if (_reader.is_open())
		{
			_reader.close();
		}
		delete _header;
		delete _tmp_next;
	}

	/*------------------------------------------------------------------\
	|-> 简述:
	|		将键值对插入到跳表中。
	|-> 返回值：
	|		false: 键值对的键已存在，插入失败。
	|		true: 键值对插入成功。
	|
	\------------------------------------------------------------------*/
	template <class K, class V>
	bool KVEngine<K, V>::insert(const K key, const V value)
	{
		mtx.lock();
		Node<K, V>* current = this->_header;
		memset(_tmp_next, 0, sizeof(Node<K, V>*) * (_max_level + 1));

		for (int i = _current_level; i >= 0; i--)
		{
			while (current->next[i] != NULL && current->next[i]->key < key)
			{
				current = current->next[i];
			}
			_tmp_next[i] = current;
		}

		current = current->next[0];

		if (current != NULL && current->key == key)
		{
#ifdef CONSOLE
			std::cout << "|-> inserted key: " << key << " already exists;" << std::endl;
#endif 
			mtx.unlock();
			return false;
		}

		if (current == NULL || current->key != key)
		{

			int random_level = get_random_level();

			if (random_level > _current_level)
			{
				for (int i = _current_level + 1; i < random_level + 1; i++)
				{
					_tmp_next[i] = _header;
				}
				_current_level = random_level;
			}

			Node<K, V>* inserted_node = new Node<K, V>(key, value,
				random_level);

			for (int i = 0; i <= random_level; i++)
			{
				inserted_node->next[i] = _tmp_next[i]->next[i];
				_tmp_next[i]->next[i] = inserted_node;
			}
#ifdef CONSOLE
			std::cout << "|-> inserted key: " << key << ", value: "
				<< value << ";" << std::endl;
#endif
			_size++;
		}
		mtx.unlock();
		return true;
	}

	/*------------------------------------------------------------------\
	|-> 简述:
	|		将键值对从跳表中删除，并将删除结果打印到屏幕上。
	|-> 返回值：
	|		false: 键值对的键不存在，删除失败。
	|		true: 键值对的键存在，删除成功。
	|
	\------------------------------------------------------------------*/
	template <class K, class V>
	bool KVEngine<K, V>::del(K key)
	{
		mtx.lock();
		Node<K, V>* current = this->_header;
		memset(_tmp_next, 0, sizeof(Node<K, V>*) * (_max_level + 1));

		for (int i = _current_level; i >= 0; i--)
		{
			while (current->next[i] != NULL && current->next[i]->key < key)
			{
				current = current->next[i];
			}
			_tmp_next[i] = current;
		}

		current = current->next[0];
		if (current != NULL && current->key == key)
		{
			for (int i = 0; i <= _current_level; i++)
			{
				if (_tmp_next[i]->next[i] != current)
					break;

				_tmp_next[i]->next[i] = current->next[i];
			}

			while (_current_level > 0 && _header->next[_current_level] == 0)
			{
				_current_level--;
			}

#ifdef CONSOLE
			std::cout << "|-> deleted key: " << key << ";" << std::endl;
#endif	
			_size--;
			mtx.unlock();
			return true;
		}

#ifdef CONSOLE
		std::cout << "|-> deleted key: " << key << " not exists;" << std::endl;
#endif
		mtx.unlock();
		return false;
	}

	/*------------------------------------------------------------------\
	|-> 简述:
	|		从跳表中查找键值对，并将查找结果打印到屏幕上。
	|
	\------------------------------------------------------------------*/
	template <class K, class V>
	bool KVEngine<K, V>::find(K key)
	{
		mtx.lock();
		Node<K, V>* current = _header;

		for (int i = _current_level; i >= 0; i--)
		{
			while (current->next[i] && current->next[i]->key < key)
			{
				current = current->next[i];
			}
		}

		current = current->next[0];

		if (current and current->key == key)
		{
#ifdef CONSOLE
			std::cout << "|-> found key: " << key << ", value: " <<
				current->value << ";" << std::endl;
#endif
			mtx.unlock();
			return true;
		}
#ifdef CONSOLE
		std::cout << "|-> not found key:" << key << ";" << std::endl;
#endif
		mtx.unlock();
		return false;
	}

	template <class K, class V>
	int KVEngine<K, V>::size()
	{
		return this->_size;
	}

	/*------------------------------------------------------------------\
	|-> 简述:
	|		打印 KVEngine 保存在内存里的数据。
	|
	\------------------------------------------------------------------*/
	template <class K, class V>
	void KVEngine<K, V>::print()
	{
		std::cout << "/-------------|print:begin|------------\\\n";
		Node<K, V>* node = this->_header->next[0];
		while (node != NULL)
		{
			std::cout << "|-> " << node->key << ":" << node->value
				<< ";" << std::endl;
			node = node->next[0];
		}
		std::cout << "\\-------------|print:end|--------------/\n";
	}

	/*------------------------------------------------------------------\
	|-> 简述:
	|		将保存在内存里的数据转存到磁盘指定位置上。
	|
	\------------------------------------------------------------------*/
	template <class K, class V>
	void KVEngine<K, V>::dump()
	{
		std::filesystem::path fp(FILE_PATH);
		if (!std::filesystem::exists(fp))
		{
			std::cout << "|-> " << fp << " not exists! \n";
			std::filesystem::create_directories(fp.parent_path());
			std::cout << "|-> created dir " << fp.parent_path() << ";\n";
		}

		_writer.open(FILE_PATH, std::ios::out);
		if (!_writer.is_open())
		{
			std::cout << "|-> open " << fp << " fail! \n";
			return;
		}
		std::cout << "|-> open " << fp << "!\n";

		Node<K, V>* node = this->_header->next[0];
		while (node != NULL)
		{
			_writer << node->key << ":" << node->value << "\n";
#ifdef CONSOLE
			std::cout << "|-> dump " << node->key << ":" <<
				node->value << ";\n";
#endif
			node = node->next[0];
		}

		_writer.flush();
		_writer.close();
		return;
	}

	template<class T1, class T2>
	T2 sstream(T1 input)
	{
		T2 ret;
		std::stringstream sstream;
		sstream << input;
		sstream >> ret;
		return ret;
	}

	/*------------------------------------------------------------------\
	|-> 简述:
	|		将磁盘指定位置上的数据加载到内存里。
	|
	\------------------------------------------------------------------*/
	template <class K, class V>
	void KVEngine<K, V>::load()
	{
		_reader.open(FILE_PATH);
		if (!_reader.is_open())
		{
			std::cout << "|-> load " << FILE_PATH << " failed;\n";
			return;
		}

		char line[1024];
		std::string key;
		std::string value;
		while (_reader.getline(line, 1024))
		{
			split_kv_str(line, key, value);
			if (key.empty() || value.empty())
				continue;
#ifdef CONSOLE
			std::cout << "|-> load " << key << ":" << value << ";\n";
#endif
			insert(sstream<std::string, K>(key), sstream<std::string, V>(value));

		}

		_reader.close();
	}

	template <class K, class V>
	int KVEngine<K, V>::get_random_level()
	{
		int k = 1;
		while (rand() % 2)
			k++;
		k = (k < _max_level) ? k : _max_level;
		return k;
	};

	template <class K, class V>
	void KVEngine<K, V>::split_kv_str(const std::string& str,
		std::string& key, std::string& value)
	{
		if (!is_valid(str))
			return;
		key = str.substr(0, str.find(_sep));
		value = str.substr(str.find(_sep) + 1, str.length());
	}

	template <class K, class V>
	bool KVEngine<K, V>::is_valid(const std::string& str)
	{
		if (str.empty() || str.find(_sep) == std::string::npos)
		{
			return false;
		}
		return true;
	}

}

#endif