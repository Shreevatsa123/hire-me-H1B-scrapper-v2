#!/bin/bash
airflow db init
airflow webserver & airflow scheduler
