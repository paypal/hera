#include <stdio.h>
#include <iostream>
#include <fstream>
#include <list>
#include "./SQLRewriter.h"
using namespace std;

int loadSQL(std::list<string> &_input){
	ifstream inputFile("sql.txt"); 
	if (inputFile.is_open()) {
		string line;
		while (getline(inputFile, line)) {
			_input.push_back(line);
		}

		inputFile.close();
	} else {
		cerr << "Error: Unable to input file [sql.txt]" << endl;
	}
	return 0;
};



int main(int argc, char *argv[]) {

	//const string shardkey = "root_payment_id";
	//const string shardkey = "account_number";
	//const string shardkey = "party_id";
	const string scuttle_attr = "scuttle_id"; 
	string shardkey;
	int i = 0;
	if (argc == 1) {
		shardkey = "account_number";
	} else {
		shardkey = argv[1];
	}
	cout<<"*****************************************\n";
	cout<<"shardkey: ["<<shardkey<<"]\n";
	cout<<"*****************************************\n\n";
	list<string> user_input;
	loadSQL(user_input);
	SQLRewriter rewriter;
	rewriter.init(shardkey, scuttle_attr);

	for (list<string>::iterator it=user_input.begin(); it != user_input.end(); ++it)
	{
		string input = *it;
		if (input.length() == 0)
			continue;
		const string * rewritten_sql = 0;
		bool sql_rewritten = false;
		int err = 0;
		rewriter.rewrite(input, rewritten_sql, sql_rewritten, err);
	
		if (sql_rewritten) {
			cout<<"SQL is modified:\n"<<*rewritten_sql<<endl<<"\nOriginal SQL:\n"<<input<<"\n\n";
		} else {
			cout<<"Use original:\n"<<input<<"\n\n";
		}
		cout<<"************************************************************************************************************************\n";
	}
	return 0;
}
