# Data pipelines using AWS Cloud - Data Engineering Project

## Table of Contents

- [Introduction](#introduction)
- [System Architecture](#system-architecture)

## Introduction

My project involves using `AWS Step Function` to automate a data processing workflow. I crawl data from the comic web and process data, then create tables to do some statistics about the comic web. <br>
Link: https://truyenmoi.info/

## System Architecture

<img src="img/workflow.jpeg" width="700">

I crawl data from the comic web like name, link, author, view,... and load data to `raw_zone` by using `AWS Lambda`. <br>
I used `EventBridge` for cyclely calling `AWS Lambda` each day. <br>
Then, I use `AWS Glue` to get data from `raw_zone`. After that, I handle this data to create 3 parquet files which name are new_table, total_view and view_of_type. And, I load this files into `golden_zone`. <br>
Finally, `Crawler Glue` gets data from `golden_zone`, handle and create 3 insight table. <br>
ALl data is stored in `AWS S3 Storage`.
