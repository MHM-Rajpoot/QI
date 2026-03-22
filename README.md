# FE College Enrolment Forecasting Dashboard

Video URL: https://www.youtube.com/watch?v=YOUR_VIDEO_ID

## Overview

This application helps colleges move from raw enrolment data to better planning decisions.

It brings together:

- historical enrolment analysis
- future demand forecasting
- provider and subject-level planning views
- Programme Plans review
- data refresh and model retraining tools

This guide focuses on what the application does, why it is useful, and how each nav bar tab supports college planning. For the technical repository guide, see `README_CODEBASE.md`.

## Why This App Matters

Colleges need more than static reports. They need a way to understand what has happened, what is changing, and what may happen next.

This app helps by:

- giving a single place to review historic enrolment trends and future demand signals
- helping leaders and planning teams compare current provision with likely future need
- improving confidence in staffing, timetable, curriculum, and budget decisions
- making it easier to review providers, campuses, levels, and subject areas with consistent data
- linking forecasting insight to Programme Plans so planning is more practical and actionable

## How Forecasting Supports Colleges

Forecasting helps colleges plan before pressure becomes a problem.

It supports colleges by helping them:

- estimate future learner demand
- prepare staffing and room capacity earlier
- review whether current curriculum plans match likely future need
- identify subject areas that may grow, flatten, or decline
- make budget and resource decisions with a forward-looking view rather than only a historical one

In practice, forecasting helps answer questions like:

- where is learner demand likely to grow?
- which provider or campus may need more delivery capacity?
- which curriculum areas may need review, expansion, or redesign?
- where do current plans appear out of line with likely demand?

## Future Addition: AI Assistant Agent Chatbot

A valuable future addition to this project would be an AI assistant agent chatbot that helps users make planning decisions based on historical data, forecast outputs, and future goals.

What this assistant could do:

- answer natural-language questions about trends, providers, curriculum areas, and learner demand
- explain forecast outputs in simpler planning language
- highlight possible risks, growth areas, and capacity pressures
- suggest actions based on the current dashboard context
- help users compare current provision with future targets and goals
- support decision-making for staffing, curriculum planning, budget review, and delivery planning

How this would help colleges:

- makes the dashboard more accessible to non-technical users
- turns data and charts into practical planning advice
- helps teams move faster from analysis to action
- supports future-focused planning conversations instead of only historical reporting

Examples of future chatbot questions:

- which subject areas are forecast to grow most over the next few years?
- which provider may need more delivery capacity?
- where do our programme plans look weak compared with forecast demand?
- what should we review first if we want to align delivery with future learner demand?

Important note:

- this is a future enhancement idea for the project, not a feature in the current application build

## Nav Bar Tabs

The main navigation bar is organized around the core planning views in the app:

- Dashboard
- Demographics
- Forecast
- College Forecast
- Data Management
- Programme Plans

### Dashboard

What this tab does:

- shows high-level enrolment KPIs
- shows the latest learner picture
- displays historical enrolment trends
- overlays forecast data on historical data
- shows provider and SSA breakdowns

Why this tab is useful:

- gives a quick planning summary for leadership and analysts
- helps users see whether demand is stable, increasing, or declining
- supports evidence-based conversations about providers, places, and subject areas

How forecasting helps here:

- the combined historical and forecast view shows where current trends may continue or shift
- changing the model helps users compare different future scenarios against the same history

### Demographics

What this tab does:

- shows learner distribution by age group
- shows learner distribution by qualification level
- works with the shared dashboard filters

Why this tab is useful:

- helps colleges understand the shape of their learner population, not only the total volume
- supports better planning for learner support, recruitment, and curriculum design
- gives context for what future growth or decline may mean operationally

How forecasting helps here:

- demographic patterns help users interpret what forecast growth may require in practice
- if demand rises, this view helps colleges judge whether that demand is likely to affect learner age mix or qualification profile

### Forecast

What this tab does:

- shows forecast results for the selected model
- compares models side by side
- shows model accuracy metrics
- displays subject-area forecast views

Why this tab is useful:

- gives a direct forward-looking planning view
- helps users compare different forecast approaches instead of relying on a single assumption
- supports curriculum, finance, and operational planning conversations with clearer evidence

How forecasting helps here:

- model comparison shows whether different methods point in the same direction
- accuracy metrics help users judge model confidence
- subject-area forecasting helps colleges identify where provision may need expansion, caution, or review

### College Forecast

What this tab does:

- drills forecasting down to provider level
- supports filtering by funding scheme, subject area, course, and level
- helps users narrow the view to specific planning questions

Why this tab is useful:

- supports detailed local planning instead of only whole-system analysis
- helps teams understand where pressure or decline may sit inside a specific provider
- makes forecast analysis more practical for staffing, curriculum, and site-level decisions

How forecasting helps here:

- provider-level forecasts support staffing and timetable planning
- course and subject filters help identify where curriculum demand may strengthen or weaken
- funding filters help users understand likely pressure across different funding-related delivery areas

### Data Management

What this tab does:

- shows database connection status
- refreshes local datasets
- retrains model outputs
- shows schema metadata
- shows current data and model resources
- allows connection details to be reviewed and updated

Why this tab is useful:

- keeps the dashboard trustworthy by making refresh and retraining visible
- supports repeatable reporting and planning cycles
- reduces reliance on manual technical steps outside the app

How forecasting helps here:

- forecasts are only valuable when the source data and saved model outputs are current
- this tab is what keeps the forecasting layer refreshed, usable, and transparent

### Programme Plans

What this tab does:

- shows Programme Plans data in a searchable, filterable explorer
- supports quick filters and advanced column filters
- supports server-side pagination for large datasets
- allows the current snapshot to be downloaded

Why this tab is useful:

- helps colleges review planned provision in a structured and practical way
- supports checks across sites, levels, approval status, parent groupings, and other planning dimensions
- makes it easier to see whether planned delivery still looks sensible against wider demand patterns

How forecasting helps here:

- forecasts can be used alongside Programme Plans to judge whether planned delivery volume looks realistic
- where forecasts suggest growth, this tab helps users check whether planned hours and structure support that demand
- where forecasts suggest decline, this tab helps users identify provision that may need review or reshaping

## Shared User Features

### Shared Filters

Several planning tabs share the same filter pattern so users can keep the same decision context while switching views.

These filters support analysis by:

- academic year range
- location
- selected forecast model

This helps users move between trend, demographic, and forecast views without losing the context of the planning question.

These shared filters are used on:

- Dashboard
- Demographics
- Forecast
- College Forecast

They are not part of the `Data Management` or `Programme Plans` tabs, because those tabs serve different operational purposes.

### Model Selector

The nav bar model selector lets users switch between the available forecasting models while staying on the current analysis view.

This is useful because:

- it encourages comparison instead of over-reliance on one model
- it helps users check whether different methods support the same planning conclusion

### Async Admin Actions

Long-running actions such as data refresh and model retraining run in the background.

This helps users because:

- the app stays responsive during heavy operations
- users can see progress and job output
- operational tasks are easier to track and understand

Admin job lifecycle:

- `queued`
- `running`
- `completed`
- `failed`

Important note:

- job history is stored in memory, so it is lost if the Flask process restarts

## Typical Planning Workflows

### Review Overall Demand

1. start on the `Dashboard` tab
2. set the year range and location context
3. review the trend and summary cards
4. switch models if you want to compare future direction

This workflow is useful for quick leadership review and early planning conversations.

### Review Future Demand In Detail

1. move to the `Forecast` tab
2. compare the available forecast models
3. review subject-area outlook
4. use the results to identify growth, risk, or review areas

This workflow is useful for curriculum and planning teams.

### Choose The Right Model View

1. open the `Forecast` tab
2. compare ARIMA, SARIMA, and LSTM outputs
3. review model accuracy metrics
4. choose the model view that best supports the planning discussion

Quick model guidance:

- `ARIMA`: useful for simpler trend patterns and easy interpretation
- `SARIMA`: useful when academic-cycle seasonality matters most
- `LSTM`: useful when users want a more flexible nonlinear view, while accepting that it is less interpretable

### Plan At Provider Or Course Level

1. open the `College Forecast` tab
2. narrow the view to a provider
3. refine by funding scheme, subject area, course, or level
4. review likely future demand for that slice

This workflow is useful for local operational planning, staffing, and provision review.

### Review Delivery Plans Against Demand

1. open the `Programme Plans` tab
2. filter to the relevant site, level, status, or grouping
3. compare planned provision against what forecast views are suggesting
4. identify areas where plans may need adjustment

This workflow is useful for turning forecast insight into actual planning action.

### Refresh Data And Rebuild Forecasts

1. open the `Data Management` tab
2. refresh the datasets
3. retrain the model outputs
4. return to the planning tabs to review updated results

This workflow is useful when the underlying data has changed and planning needs the latest picture.

## Workflow Diagrams

### Strategic Planning Flow

```text
Dashboard
   |
   v
Forecast
   |
   v
College Forecast
   |
   v
Programme Plans
   |
   v
Planning Decision
```

What this shows:

- start with the whole-picture demand view
- move into future-demand analysis
- narrow to provider or curriculum detail
- compare likely demand with planned delivery
- turn the result into a staffing, curriculum, or resource decision

### Operational Refresh Flow

```text
Data Management
   |
   +--> Refresh Data
   |       |
   |       v
   |   Updated Local Datasets
   |
   +--> Train Models
           |
           v
     Updated Forecast Outputs
           |
           v
Dashboard / Forecast / College Forecast / Programme Plans
```

What this shows:

- `Data Management` is the operational control point
- refreshed data feeds the local datasets
- retraining updates the saved forecast outputs
- the planning tabs then use the refreshed picture

### Forecast Output Flow

```text
Refresh Data
   |
   v
Updated Local CSV Datasets
   |
   v
Train Models
   |
   v
Saved Forecast Outputs
   |
   v
Forecast Tabs And Planning Views
```

What this shows:

- forecast outputs depend on refreshed source data
- retraining converts refreshed datasets into saved forecast artifacts
- the planning tabs then use those updated outputs

### Provider Planning Flow

```text
Dashboard
   |
   v
Demographics
   |
   v
College Forecast
   |
   v
Programme Plans
   |
   v
Local Delivery Review
```

What this shows:

- start with the whole-picture trend
- understand learner mix
- narrow to provider, course, or funding slice
- compare likely demand with planned delivery
- review whether local provision still looks right

## How The Application Works In Practice

The app is designed to feel like one planning workspace rather than a collection of disconnected reports.

In practice:

- historical views help explain what has happened
- forecast views help estimate what may happen next
- provider and subject filters help narrow the question
- Programme Plans help connect insight to real delivery planning
- Data Management keeps the dataset and model outputs up to date

This makes the application useful for both strategic review and day-to-day planning decisions.

## What Train Models Produces

When users retrain the models, the app updates saved forecasting outputs used across the planning tabs.

Typical outputs include:

- ARIMA forecast rows
- SARIMA forecast rows
- LSTM forecast rows
- subject-area forecast outputs
- model metric files for forecast comparison and accuracy review

This matters because the `Forecast` and `College Forecast` views become more useful when these saved outputs reflect the latest refreshed data.

## Running The App Locally

### Install Dependencies

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
```

## Troubleshooting

### Refresh Data Is Not Working

Check the `Data Management` tab for:

- database status
- refresh job progress
- output log messages

### Forecast Results Look Empty

Possible reasons:

- the latest data has not been refreshed
- model outputs have not been retrained
- the current filters are too narrow
- the selected planning slice does not have enough matching data

### Programme Plans Looks Empty

Check:

- active filters
- search terms
- whether the latest snapshot has been refreshed

The Programme Plans view is paginated on purpose, so it does not load the full dataset into the browser at once.

## Operational Notes

- data refresh and model retraining run asynchronously
- job progress is visible in the app
- forecasts are most useful when refresh and retraining are part of a regular planning cycle
- Programme Plans is designed to support large datasets without loading every row at once
- retraining updates saved forecast artifacts, not just on-screen visuals

## Mentors

The project is supported by the following mentors:

- [Jerome Wittersheim](https://www.linkedin.com/in/jerome-wittersheim/)
- [Rhys Spence](https://www.linkedin.com/in/rhys-spence-education-adviser/)
- [Caryn Swart](https://www.linkedin.com/in/caryn-swart-9915567/)

## Developer Details

- Developer: Muhammad Hassan Mukhtar
- Email: [rajpootmhm@gmail.com](mailto:rajpootmhm@gmail.com)
- Phone: `+44 7350536488`
- LinkedIn Profile: [linkedin.com/in/-muhammad-hassan-mukhtar-/](https://www.linkedin.com/in/-muhammad-hassan-mukhtar-/)
