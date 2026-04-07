# IoT Irrigation Control System

## Overview

This project is a smart irrigation system developed from web application to microcontroller control.
It enables remote monitoring and control in rural and mountainous areas using 429MHz communication.

## Background

In rural areas, network infrastructure is often unstable, making conventional IoT solutions difficult to deploy.
This system was designed to operate under such constraints using low-bandwidth communication.

## Features

* Remote irrigation control via web application
* Device management on map interface
* Communication status monitoring (RSSI)
* Fail-safe control under unstable network conditions

## System Architecture

* Backend: Flask (Python)
* Frontend: HTML / JavaScript
* Device: ESP32 / Arduino
* Communication: 429MHz

## Design Considerations

* Role-based access control for different users (farmers, administrators)
* Device visibility restriction based on assigned permissions
* Fail-safe local control when communication is lost
* Command and ACK mechanism for reliable state management

## Prediction Module

The `predict.py` and `predict.html` components provide a framework for estimating communication strength and optimizing device placement.
The detailed prediction algorithm is under development.

## Future Work

* Local control using flow sensors
* AI-based irrigation optimization
* Advanced communication prediction model

## Security

This repository is a public portfolio version.
Sensitive information such as credentials, tokens, and production configurations has been removed.

## License

This project is created for portfolio purposes.
