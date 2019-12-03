//
// File: wl.h
// 
//  Description: Clarification file for homework 1 
//  Student Name: Sirui Yi
//  UW Campus ID: 9075297896
//  enamil: syi26@wisc.edu 

#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <vector>
#include <cstdio>
#include <iterator>

using namespace std;

/**
 * Declarations of functions that would be used
 */

void Load_func(string file_name, vector<string> &word_set);
int Locate_func(string target, int num, vector<string> &word_set);
void New_func(vector<string> &word_set);
vector<string> parse(string command);

/**
 * Main function that deals mainly with command line arguments
 */

int main() {

    /**
     * word_set: the data structure that save the words in inputfile
     * command: the command line
     * command_set: the parsed command
     */
    vector<string> word_set;
    string command;
    vector<string> command_set;

    /**
     * dealing with each case that would be encountered
     */

    for (;;) {
        cout << ">";
        getline(cin, command);
        if (command.empty()) {
            continue;
        }
        for (unsigned i = 0; i < command.size(); i++) {
            if (isalpha(command[i])) {
                command[i] = tolower(command[i]);
            }
        }
        command_set = parse(command);

        if (command_set[0].compare("load") == 0) {
            if (command_set.size() != 2) {
                cout << "ERROR: Invalid command" << endl;
                continue;
            }
            Load_func(command_set[1], word_set);
        } else if (command_set[0].compare("locate") == 0) {
            bool flag = true;
            if (command_set.size() != 3) {
                cout << "ERROR: Invalid command" << endl;
                continue;
            }
            for (unsigned i = 0; i < command_set[2].size(); i++) {
                if (!isdigit(command_set[2][i])) {
                    flag = false;
                    break;
                }
            }
            for (unsigned i = 0; i < command_set[1].size(); i++) {
                if (ispunct(command_set[1][i]) && command_set[1][i] != '\'') {
                    flag = false;
                    break;
                }
            }
            if (!flag || stoi(command_set[2]) == 0) {
                cout << "ERROR: Invalid command" << endl;
                continue;
            }

            int loc = Locate_func(command_set[1], stoi(command_set[2]), word_set);
            if (loc == -1) {
                cout << "No matching entry" << endl;
            } else {
                cout << ++loc << endl;
            }
        } else if (command_set[0].compare("new") == 0) {
            if (command_set.size() != 1) {
                cout << "ERROR: Invalid command" << endl;
                continue;
            }
            New_func(word_set);
        } else if (command_set[0].compare("end") == 0) {
            if (command_set.size() != 1) {
                cout << "ERROR: Invalid command" << endl;
                continue;
            }
            word_set.clear();
            return 0;
        } else {
            cout << "ERROR: Invalid command" << endl;
        }
    }
}

/**
 * method for parsing a line of string according to ' '
 * returns a vector of parsed strings
 */

vector<string> parse(string command) {
    stringstream ss(command);
    istream_iterator<string> begin(ss);
    istream_iterator<string> end;
    vector<string> vstrings(begin, end);
    return vstrings;
}

/**
 * function for loading file in, which takes the file name and 
 * the data structure for restoring words as input
 */

void Load_func(string file_name, vector<string> &word_set) {
    /**
     * read file, check if it exists or not
     */
    ifstream input_file (file_name);
    if (!input_file) {
        cout << "ERROR: Invalid command" << endl;
        return;
    }

    /**
     * clear the data structure when a valid file is read in
     */
    word_set.clear();
    
    /**
     * load the file into data structure
     */
    vector<string> words;
    string line;

    while (getline(input_file, line)) {
        if (line.empty()) {
            continue;
        }

        /**
         * change punctuations other than single quotes to blackspace
         * change upper case letters to lower case letters for comparisons
         */
        for (unsigned i = 0; i < line.size(); i++) {
            if (ispunct(line[i]) && line[i] != '\'') {
                line[i] = ' ';
            }
            if (isalpha(line[i])) {
                line[i] = tolower(line[i]);
            }
        }
        words = parse(line);
        for (unsigned i = 0; i < words.size(); i++) {
            word_set.push_back(words[i]);
        }
    }

    /**
     * close the file stream after loading
     */
    input_file.close();
    return;
}

/**
 * function for locating desired words
 * takes a target word, an integer and the data structure as inputs
 */

int Locate_func(string target, int num, vector<string> &word_set) {
    int num_visit = 0;
    int count = 0;

    /**
     * change upper case letters to lower case letters for comparison
     */
    for (unsigned i = 0; i < target.size(); i++) {
        if (isalpha(target[i])) {
            target[i] = tolower(target[i]);
        }
    }

    /**
     * traverse the data structure to find the target word
     */
    for (string word:word_set) {
        if (target.compare(word) == 0) {
            num_visit++;
        }
        if (num_visit == num) {
            return count;
        }
        count++;
    }
    return -1;  // didn't find the target
}

/**
 * function for clearing the data structure
 */

void New_func(vector<string> &word_set) {
    word_set.clear();
}
