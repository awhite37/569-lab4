#To Run:

go build mr/mr.go

go build rf/*.go

go build -buildmode=plugin wc.go

go build main.go worker.go fmanager.go 

./main input_files/pg-metamorphosis.txt wc.so 

#To compare output to sequential: 

go build mrsequential.go

./mrsequential wc.so ./input_files/pg-metamorphosis.txt 

diff mr-out-0 mr-seq-out-0 

