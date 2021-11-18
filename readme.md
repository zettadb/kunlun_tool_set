# Kunlun Cluster Tool Set

## Description

This project aims to provide the utility for Kunlun-Cluster aka 'KTS'

Including but not limited to backup/restore tools...

## Structure

1.The 'cmd' folder contains the utility's main package, if you want to check the toolset Bird's eye view, that is the
right path.

2.The 'conf' folder contains the config file template which is self-explanatory.

3.The 'util' folder contains the implementation details

## Code of conduct

1.The function of the tool which provied in this project should follow the `KISS` principle

2.The specification for the tool is self should be fetched by invoking tool by --help

3.TOML is the only config file format which used by the tool

4.Keep main package simple, all the logic should be implemented under 'util' folder