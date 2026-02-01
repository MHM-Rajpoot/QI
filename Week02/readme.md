# Week 02

# UK Job Data Collection & Pre-processing (Glassdoor)

## Overview

This week focuses on **collecting authentic, real-time job market data** to complement the official UK labour statistics analyzed in Week 01.  
The primary objective is to gather **job-level information** (roles, skills, salaries, experience levels, and posting dates) from an online job platform and prepare it for downstream analysis.

**Glassdoor** was selected as the main data source due to its rich job descriptions, salary insights, and employer-reported information, which together provide a granular view of current labour demand.

---

## Data Source

### Glassdoor – Job Listings Platform

Glassdoor provides detailed job postings that include:

- Job title and role  
- Required skills and technologies  
- Salary ranges  
- Experience and seniority levels  
- Posting dates  
- Company and industry context  

This data serves as a **market-driven perspective** on labour demand, complementing official datasets such as ONS and NOMIS.

---

## Web Scraping Approach

Since Glassdoor does not provide unrestricted public APIs for bulk job data access, **web scraping** was used to extract job listings.

### Tools and Techniques

- Automated browser-based scraping  
- HTML parsing of job listings  
- Structured extraction of:
  - Job descriptions  
  - Skills  
  - Salary information  
  - Posting dates  

---

## Scraping Challenges and Issues

Scraping Glassdoor presents several technical and ethical challenges due to strong anti-bot mechanisms.

### 1. Bot Detection Mechanisms

Glassdoor actively detects automated behavior using:

- Cloudflare authentication  
- IP reputation checks  
- JavaScript challenges  
- Request rate monitoring  
- Browser fingerprinting  

These measures often result in:

- CAPTCHA challenges  
- Temporary IP bans  
- Access denial pages  

---

### 2. Cloudflare Authentication

Cloudflare acts as a protective layer that:

- Blocks non-human browsing patterns  
- Requires JavaScript execution  
- Flags repeated or predictable request behavior  

This makes traditional static scraping approaches ineffective.

---

## Mitigation Strategies

To reduce detection risk, **human-like interaction patterns** were introduced.

### Humanized Browsing Behaviour

The scraping process simulates realistic user actions, including:

- Randomized scrolling behavior  
- Natural mouse movements  
- Random delays between actions  
- Controlled clicking patterns  
- Waiting times between page loads  

These techniques help mimic genuine user interaction and reduce the likelihood of triggering bot detection systems.

| Bot Signal        | Human Behavior |
| ----------------- | -------------- |
| Perfect timing    | Random delays  |
| Clicks everything | Skips cards    |
| Instant clicks    | Hover delay    |
| No idle time      | Random pauses  |
| Static cursor     | Mouse movement |
| Fast pagination   | Slow browsing  |

---

## Data Cleaning and Transformation

The scraped data is initially unstructured and text-heavy, requiring extensive preprocessing.

### Tabular Data Construction

Key information is extracted and organized into structured tables:

- Job title  
- Company  
- Location  
- Salary (if available)  
- Skills (from job descriptions)  
- Posting date  
- Job description text  

---

### Skills and Date Extraction

- Skills are parsed from job descriptions using keyword-based extraction  
- Posting dates are standardized into a uniform date format  
- Irrelevant or duplicate entries are removed  

---

## Rule-Based Data Enrichment

To improve data usability, **rule-based logic** is applied.

### Job Level Assignment

When explicit seniority is missing, job levels are inferred using salary thresholds:

- Entry-level  
- Mid-level  
- Senior-level  

This rule-based filling ensures consistency across listings and enables meaningful comparison between roles.

