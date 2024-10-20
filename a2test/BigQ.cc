#include "BigQ.h"
#include <thread>
using namespace std;

//BigQ Constructor
//  create 1 worker thread
//      Constantly call inputPipe.remove to get all records
//          Sort with TPMMS
//              Accept run length pages of records
//              Sort these records
//                  Use the OrderMaker class to determine sorted order.
//              Write out sorted records as a run
//              Append all runs to the same file
//  return from constructor
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortOrder, int runlen) : 
    in(in), out(out), runLength(runlen), sortOrder(sortOrder) {
    // Create thread
    //cout << "Create worker thread\n";
    thread worker(&BigQ::sortWorker, this);
    //cout << "Worker thread created\n";

    if (worker.joinable()) {
        std::cout << "Worker thread initialized successfully\n";
    }

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe
    // finally shut down the out pipe
    worker.detach();
}

void BigQ::sortWorker() {
    cout << "Worker thread is running...\n";
    Record* record = new Record();
    std::vector<Record*> records;
    //std::vector<off_t> runLocation;
    ComparisonEngine compare;
    int recordCap = runLength * PAGE_SIZE;
    File file;
    char* fileName = "records.txt";
    file.Open(0, fileName);
    int numPages = 0;

    // Loop while there are still records in the pipe
    while (this->in.Remove(record) != 0) {
        //runLocation.push_back(file.GetLength());
        for (int numRecords = 0; numRecords < recordCap; numRecords++) {
            records.push_back(record);
            numRecords++;
            if (this->in.Remove(record) == 0) {
                break;
            }
            //cout << "Grabbing Record.\n";
        }
        //sort vector
        std::sort(records.begin(), records.end(), [&](Record* left, Record* right) {
            return compare.Compare(left, right, &sortOrder) < 0;
        });
        // Push vector to file
        for (Record* record : records) {
            Record* temp = new Record();                    ///////////////////
            temp->Copy(record);                             // Need to remove somehow... Deep copy is 
            //cout << "Next record: " << &temp << endl;     // expensive and unessesarry
                                                            ///////////////////
            // If page is full push it to the file.
            if (page.Append(temp) == 0) {
                cout << "Next page\n";
                file.AddPage(&page, file.GetLength());
                numPages++;
                page.EmptyItOut();
                page.Append(temp);
            }
            cout << "Appended Record to page\n";
            free(temp);
        }
        if (!page.IsEmpty()) {
            file.AddPage(&page, file.GetLength());
            cout << "Added page to file\n";
            cout << "There are " << page.GetNumRecs() << " records on the page\n";
            numPages++;
        }
        page.EmptyItOut();
        records.clear();
    }

    // TODO PHOEBE ////////////////////////////////TODO PHOEBE//////////////////////////////
    // Implement part 2 of TPMMS
    // load the first element of each run and push the smallest to out
    // replace the pushed element with the next smallest and repeat
    // until all elements are pushed to out
    // The start of each run is stored in off_t runLocation

    Page* page_ptr = new Page();
    cout << "numPages: " << numPages << endl;
    for (int i = 0; i < numPages; i++) {
        cout << "There are " << file.GetLength() << " pages in the file\n";
        file.GetPage(page_ptr, i);
        page_ptr->MoveToFirst();
        while (page_ptr->GetFirst(record) != 0) {
            out.Insert(record);
        }
        //cout << "Inserted record in output pipe\n";
    }
    cout << "Worker Done" << endl;
    file.Close();
    out.ShutDown();
    
}
// hello world

BigQ::~BigQ () {
    std::cout << "BigQ destructor called" << std::endl;
    if (worker.joinable()) {  // Check if the worker thread is still running
        worker.join();        // Wait for the worker thread to finish
    }
}

int BigQ::sort (std::vector<Record*> &records){
	return 0;
}