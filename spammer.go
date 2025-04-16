package main

import (
	"fmt"
	"sort"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	var wg sync.WaitGroup
	var nextIn chan interface{} = nil
	for _, c := range cmds {
		outChan := make(chan interface{})
		wg.Add(1)
		go func(cmdFunc cmd, in, out chan interface{}) {
			defer wg.Done()
			cmdFunc(in, out)
			close(out)
		}(c, nextIn, outChan)
		nextIn = outChan
	}
	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	var wg sync.WaitGroup
	seen := make(map[uint64]struct{})
	var mu sync.Mutex

	for emailRaw := range in {
		email := emailRaw.(string)
		wg.Add(1)
		go func(e string) {
			defer wg.Done()
			user := GetUser(e)
			mu.Lock()
			defer mu.Unlock()
			if _, exists := seen[user.ID]; !exists {
				seen[user.ID] = struct{}{}
				out <- user
			}
		}(email)
	}

	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	batchChan := make(chan []User)

	go func() {
		defer close(batchChan)
		var batch []User
		for uRaw := range in {
			user := uRaw.(User)
			batch = append(batch, user)
			if len(batch) >= GetMessagesMaxUsersBatch {
				batchChan <- batch
				batch = nil
			}
		}
		if len(batch) > 0 {
			batchChan <- batch
		}
	}()

	var wgWorkers sync.WaitGroup
	for i := 0; i < 5; i++ {
		wgWorkers.Add(1)
		go func() {
			defer wgWorkers.Done()
			for batch := range batchChan {
				msgIDs, err := GetMessages(batch...)
				if err != nil {
					continue
				}
				for _, msgID := range msgIDs {
					out <- msgID
				}
			}
		}()
	}

	wgWorkers.Wait()
}

func CheckSpam(in, out chan interface{}) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, HasSpamMaxAsyncRequests)

	for msgIDRaw := range in {
		msgID := msgIDRaw.(MsgID)
		wg.Add(1)
		go func(m MsgID) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			hasSpam, err := HasSpam(m)
			if err != nil {
				return
			}
			out <- MsgData{
				ID:      m,
				HasSpam: hasSpam,
			}
		}(msgID)
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var results []MsgData
	for dataRaw := range in {
		results = append(results, dataRaw.(MsgData))
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].HasSpam != results[j].HasSpam {
			return results[i].HasSpam
		}
		return results[i].ID < results[j].ID
	})

	for _, res := range results {
		out <- fmt.Sprintf("%t %d", res.HasSpam, res.ID)
	}
}
