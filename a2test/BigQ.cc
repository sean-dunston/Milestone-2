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
    int recordCap = runLength * PAGE_SIZE / sizeof(record);
    File file;
    char* fileName = "records.txt";
    file.Open(0, fileName);
    int numPages = 0;
    int numRuns = 0;
    Record* temp = new Record();
    Record* toAdd = new Record();

    // Loop while there are still records in the pipe
    while (this->in.Remove(record) != 0) {
        //runLocation.push_back(file.GetLength());
        numRuns++;
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
            //cout << "Appended Record to page\n";
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
    // The start of each run can be found with run# * runlength.
    // If you run out of pages in a run, ignore this run in future checks.

    Page* page_ptr = new Page();
    std::vector<Record*> recArray(numRuns);
    std::vector<int> pageOffset(numRuns, 0);
    std::vector<Page*> pageArray(numRuns);
    //Record* temp = new Record();
    int totalPages = file.GetLength();
    for (int run = 0; run < numRuns; run++) {
        bool valid = false;
        cout << "Run " << run + 1 << " of " << numRuns << endl;
        // Make sure we have a valid record and add it to recArray, if not, find the next valid record
        // If we do not have the current page of this run, store it in the array
        while (!valid) {
            
            // check to see if there are any more records in the run
            if (pageOffset[run] < runLength) {
                if (pageArray[run] == nullptr) {
                //cout << "Page number = " << (page_ptr, run * runLength + pageOffset[run]) << endl;
                file.GetPage(page_ptr, run * runLength + pageOffset[run]);
                pageArray[run] = page_ptr;
                }
                pageArray[run]->MoveToFirst();
                // Check to see if we have exhausted the page and increment to the next one
                cout << "Getting first record of the run\n";
                cout << "There are " << pageArray[run]->GetNumRecs() << " records on the page\n";
                if(pageArray[run]->GetFirst(temp) == 0) {
                    pageOffset[run]++;
                    pageArray[run] = nullptr;
                } else {
                    valid = true;
                    recArray[run] = temp;
                    cout << "Smallest record of run added to the array\n";
                }
            } else {
                recArray[run] = nullptr;
                valid = true;
            }
            //recArray[run] = //the first record of the run's first page;
        }
    }
    cout << "Finished initial array\n";
    while (true) {
        auto minRec = std::min_element(recArray.begin(), recArray.end(), [&](Record* left, Record* right) {
            return compare.Compare(left, right, &sortOrder) == 1;
        });
        int minIndex = std::distance(recArray.begin(), minRec);
        //recArray[minIndex] 
        //cout << "At position " << minIndex << " of " << recArray.size() << endl;
        if (!recArray.empty() && minRec != records.end()) {
            //cout << "Output record\n";
            //out.Insert(*minRec);
            out.Insert(recArray[minIndex]);
        } else {
            cout << "minRec was empty\n";
            break;
        }
        //pageArray[minIndex]->GetFirst(recArray[minIndex]);
        //////////////////////////////////////////////////////////////////////////////
        bool valid = false;
        while (!valid) {
            int pageNumber = minIndex * runLength + pageOffset[minIndex];
            cout << "Page number = " << pageNumber << " of " << totalPages << " pages\n";
            cout << "Min Index = " << minIndex << ", Page Offset = " << pageOffset[minIndex] << endl;
            // check to see if there are any more records in the run
            if (pageOffset[minIndex] < runLength && pageNumber < totalPages - 1) {
                if (pageArray[minIndex] == nullptr) {
                //cout << "Page number = " << pageNumber << " of " << totalPages << " pages\n";

                file.GetPage(page_ptr, pageNumber);
                pageArray[minIndex] = page_ptr;
            }
                // Check to see if we have exhausted the page and increment to the next one
                //Record* toAdd = new Record();
                pageArray[minIndex]->MoveToFirst();
                if(pageArray[minIndex]->GetFirst(toAdd) == 0) {
                    cout << "increment page\n";
                    pageOffset[minIndex]++;
                    pageArray[minIndex] = nullptr;
                } else {
                    recArray[minIndex] = toAdd;
                    valid = true;
                    cout << "Continued without incrementing page\n";
                }
            } else if (!recArray.empty()) {
                recArray.erase(recArray.begin() + minIndex);
                pageOffset.erase(pageOffset.begin() + minIndex);
                pageArray.erase(pageArray.begin() + minIndex);
                valid = true;
            } else {
                valid = true;
            }
            //recArray[run] = //the first record of the run's first page;
        }
    }
    /*
    Page* page_ptr = new Page();
    cout << "numPages: " << numPages << endl;
    for (int i = 0; i < numPages; i++) {
        //cout << "There are " << file.GetLength() << " pages in the file\n";
        file.GetPage(page_ptr, i);
        page_ptr->MoveToFirst();
        while (page_ptr->GetFirst(record) != 0) {
            out.Insert(record);
        }
        //cout << "Inserted record in output pipe\n";
    }
    */
    delete temp;
    cout << "Worker Done" << endl;
    file.Close();
    out.ShutDown();
    
}
// hello world

BigQ::~BigQ () {
    std::cout << "BigQ destructor called" << std::endl;
    if (worker.joinable()) {  // Check if the worker thread is still running
        worker.join();        // Wait for the worker thread to finish
        cout << "Worker joined\n";
    }
}

int BigQ::sort (std::vector<Record*> &records){
	return 0;
}