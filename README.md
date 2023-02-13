# Chris Kollbaum

## 2/10/2023

# streaming-05-smart-smoker

This repo is to store our module 5 project in which we create a producer that sends messages to a queue about temperatures for a meat smoker.  This will send the temperature of the smoker, temperature of food A, and temperature of food B to three separate queues to eventually be processed by three separate consumers.    

## Before You Begin

1. Create a new repo into your GitHub with a readme.md
1. Clone your repo down to your machine.
1. Use VS Code to add.gitignore.
1. Add the csv file to your repo.
1. Create a file for our bbq producer.
1. View / Command Palette - then Python: Select Interpreter

## Using a Barbeque Smoker
When running a barbeque smoker, we monitor the temperatures of the smoker and the food to ensure everything turns out tasty. Over long cooks, the following events can happen:

## The smoker temperature can suddenly decline.
The food temperature doesn't change. At some point, the food will hit a temperature where moisture evaporates. It will stay close to this temperature for an extended period of time while the moisture evaporates (much like humans sweat to regulate temperature). We say the temperature has stalled.
 

## Sensors
We have temperature sensors track temperatures and record them to generate a history of both (a) the smoker and (b) the food over time. These readings are an example of time-series data, and are considered streaming data or data in motion.

 

## Streaming Data
Our thermometer records three temperatures every thirty seconds (two readings every minute). The three temperatures are:

the temperature of the smoker itself.
the temperature of the first of two foods, Food A.
the temperature for the second of two foods, Food B.
 

## Significant Events
We want know if:

The smoker temperature decreases by more than 15 degrees F in 2.5 minutes (smoker alert!)
Any food temperature changes less than 1 degree F in 10 minutes (food stall!)
 

## Smart System
## We will use Python to:

Simulate a streaming series of temperature readings from our smart smoker and two foods.
Create a producer to send these temperature readings to RabbitMQ.
Create three consumer processes, each one monitoring one of the temperature streams. 
Perform calculations to determine if a significant event has occurred.

## Screenshot

![Emitter Code](smokersemitter.png)

![Output](smokerscreenshot2.png)

 





