package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	nasdaqIndexFile     = "./data/fh_20190420/NASDAQ.txt"
	nasdaqHistoryFormat = "./data/fh_20190420/full_history/%s.csv"
)

var (
	errInvalidCompanyFormat = errors.New("Invalid companyFormat")
)

type stockRecord struct {
	Code     string  `bson:"code,omitempty"`
	Date     string  `bosn:"date,omitempty"`
	Volume   float64 `bson:"volume,omitempty"`
	Open     float64 `bson:"open,omitempty"`
	Close    float64 `bson:"close,omitempty"`
	High     float64 `bson:"high,omitempty"`
	Low      float64 `bson:"low,omitempty"`
	AdjClose float64 `bson:"adjclose,omitempty"`
}

type companies struct {
	total int64
	name  map[string]string
}

func (c *companies) Load() error {
	if c.name != nil {
		return nil
	}

	c.name = make(map[string]string)

	f, err := os.Open(nasdaqIndexFile)

	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		info := strings.Split(string(line), "\t")
		if len(info) != 2 {
			return errInvalidCompanyFormat
		}

		if info[0] == "Symbol" {
			continue
		}

		c.name[info[0]] = info[1]
	}
}

func (c *companies) LoadHistory() error {
	for code := range c.name {
		if err := c.loadHistoryForCompany(code); err != nil {
			return err
		}
	}

	return nil
}

func (c *companies) loadHistoryForCompany(code string) error {
	log.Println("Start loading ", code)

	f, err := os.Open(fmt.Sprintf(nasdaqHistoryFormat, code))
	if err != nil {
		return err
	}
	defer f.Close()

	reader := csv.NewReader(bufio.NewReader(f))

	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://root:single@localhost:27017"))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		return err
	}
	defer client.Disconnect(context.Background())

	collection := client.Database("stocks").Collection("nasdaq")

	lines := 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}

		lines++
		if lines == 1 {
			continue
		}

		c.total++

		s := stockRecord{
			Code: code,
			Date: line[0],
		}

		s.Volume, _ = strconv.ParseFloat(line[1], 64)
		s.Open, _ = strconv.ParseFloat(line[2], 64)
		s.Close, _ = strconv.ParseFloat(line[3], 64)
		s.High, _ = strconv.ParseFloat(line[4], 64)
		s.Low, _ = strconv.ParseFloat(line[5], 64)
		s.AdjClose, _ = strconv.ParseFloat(line[6], 64)

		if _, err = collection.InsertOne(context.Background(), &s); err != nil {
			return err
		}
	}

	log.Println(code, lines, c.total)

	return nil
}

func main() {
	var c companies

	if err := c.Load(); err != nil {
		log.Fatal(err)
	}

	if err := c.LoadHistory(); err != nil {
		log.Fatal(err)
	}
}
