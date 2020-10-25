package mr

type KeyVal struct {
    Key string
    Val string
}

type KeyVals []KeyVal

func (kv KeyVals) Len() int {
	return len(kv)
}

func (kv KeyVals) Less(i, j int) bool {
	return kv[i].Key < kv[j].Key
}

func (kv KeyVals) Swap(i, j int) {
	kv[i], kv[j] = kv[j], kv[i]
}
