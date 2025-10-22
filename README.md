# hire-me-H1B-scrapper-v2

A production-ready, scalable job scraping pipeline tailored for international job seekers looking for H1B visa sponsorship roles in the United States. This project automates the discovery and extraction of relevant job listings using **Apache Airflow**, with data stored in **SQLite**, and the whole system is containerized using **Docker** for easy deployment and scalability.

---

## 📌 Objective

The goal of this project is to help users:

* Identify **H1B-relevant jobs** like *"Data Engineer"*, *"Software Engineer"*, etc.
* **Scrape** job listings automatically at scheduled intervals.
* **Store** structured job data locally using a lightweight database (SQLite).
* Easily **scale**, **maintain**, and **deploy** this scraping system across environments.

---

## 🧱 Tech Stack

| Technology     | Purpose                                    |
| -------------- | ------------------------------------------ |
| Python         | Core scripting and scraping logic          |
| Apache Airflow | DAG scheduling and orchestration           |
| SQLite         | Local lightweight database storage         |
| Docker         | Containerization for consistent deployment |
| BeautifulSoup  | HTML parsing (assumed in scraper)          |
| Requests       | Making HTTP requests to job websites       |

---

## 📂 Project Structure

```
hire-me-H1B-scrapper-v2/
│
├── dags/                       
│   ├── job_scraper_dag.py       # Main DAG for scraping and storing data
│   └── __init__.py
│
├── db/
│   └── jobs.db                  # SQLite database (auto-created)
│
├── utils/
│   ├── scraper.py               # Web scraping logic
│   ├── filters.py               # Keyword-based filtering logic
│   └── db_writer.py             # Code to insert records into SQLite
│
├── airflow_home/                # Local Airflow configs/logs (ignored in git)
│
├── docker-compose.yml           # Sets up Airflow with all services
├── .gitignore                   # Ensures airflow logs and .db files are not committed
└── README.md                    # You’re reading it!
```

---

## 🧠 How It Works

### 🕸️ 1. Web Scraper

Scrapes job listing websites and parses each listing using `BeautifulSoup`.

### 🔍 2. Keyword Filter

Filters jobs based on target roles like:

* `"data engineer"`
* `"software engineer"`
* `"H1B visa"`
  (You can extend this list in the code.)

### 🗃️ 3. Data Storage

Stores scraped jobs in a local `SQLite` database with the following schema:

| Column Name        | Description                              |
| ------------------ | ---------------------------------------- |
| `date_posting`     | Date when the job was originally posted  |
| `date_scrapped`    | Date when the scraping job was triggered |
| `job_posting_name` | Title of the job listing                 |
| `description`      | Full job posting text                    |
| `link`             | Direct link to the job post              |

### ⏱️ 4. Airflow DAG

The `job_scraper_dag.py` DAG:

* Runs daily (or as per your schedule)
* Calls the scraper script
* Filters and saves jobs to the database
* Can be extended to send email/Slack notifications

---

## ⚙️ Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/hire-me-H1B-scrapper-v2.git
cd hire-me-H1B-scrapper-v2
```

### 2. Launch with Docker

```bash
docker-compose up -d
```

This launches Airflow Scheduler, Webserver, and Database in isolated containers.

### 3. Access the Airflow UI

Visit: [http://localhost:8080](http://localhost:8080)
Login with the default credentials (configurable in `docker-compose.yml`) and trigger the DAG named `job_scraper_dag`.

---

## 📦 Customization

You can customize the project to:

* Add more scraping sources (Indeed, LinkedIn, etc.)
* Modify the filtering logic in `utils/filters.py`
* Change the job title keywords
* Export to cloud databases or data warehouses

---

## 🧪 Development Tips

* Use `airflow dags test job_scraper_dag <YYYY-MM-DD>` to test locally.
* Make sure Airflow connections and environment variables are configured inside `docker-compose.yml`.
* Set your timezone in Airflow using `AIRFLOW__CORE__DEFAULT_TIMEZONE`.

---

## 🌱 Future Enhancements

* [ ] Add support for multiple job portals
* [ ] Implement job deduplication logic
* [ ] Integrate cloud database (e.g., PostgreSQL, BigQuery)
* [ ] Add user notifications for new postings
* [ ] Build a simple Streamlit dashboard to display results

---

## 🤝 Contributing

Contributions, issues, and feature requests are welcome!
Feel free to fork the project and submit a pull request.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

## 👩‍💻 Author

**\Shreevatsa Agnihotri**
Incoming MS in Data Science student at the University of Maryland.
Former Associate Data Engineer at Shell, passionate about scalable data solutions for real-world problems.

---
