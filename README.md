# IoT Irrigation Control System

## Notes for Public Repository

This repository contains the core logic of the irrigation control system for public demonstration.

For security and privacy reasons, the public version omits parts related to:
- user login / logout
- signup
- session-based authentication
- admin account management
- role-based access control
- owner / pin access assignment

These features exist in the private production version, but are intentionally excluded here to avoid exposing sensitive implementation details and credentials.

The public code focuses on the core technical components:
- pin registration and coordinate handling
- telemetry collection
- log storage
- command distribution to devices
- ACK handling
- schedule management
- CSV export
- gateway pull API
- device communication flow


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

## Third-Party Components

This system utilizes a 429MHz wireless communication module provided by Circuit Design, Inc.
The hardware and underlying radio communication technology are the intellectual property of the company.

This project focuses on system design and software implementation built on top of these modules.


## License

This project is created for portfolio purposes.
