# lilota

## Table of contents
- [What is it?](#what-is-it)
- [When to use it?](#when-to-use-it)
- [How does it work?](#how-does-it-work)
- [How to install it?](#how-to-install-it)
- [How to use it?](#how-to-use-it)


## What is it?
**lilota** is a **li**ghtweight solution for **lo**ng-running **ta**sks. When we talk about such a solution we're essentially speaking about a tool that can handle task management efficiently without the overhead and complexity of other systems like Celery, RabbitMQ, etc.


## When to use it?
Lets assume you have a web application and you send a request to the server where you want to run long running tasks like processing images, generating reports based on large data sets, sending emails etc. 

In such cases you do not want to let the user wait for the response of the server. Instead you want to send the request to the server and let the user know that the request has been received and that the server is working on it.

Here **lilota** can help you by processing the long-running task in a separate process and returning the response to the user as soon as the task has been started.


## How does it work?


## How to install it?


## How to use it?
