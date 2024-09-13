# Streamline Engine
> **Note**: This README is AI generated, so it might be incomplete or inaccurate. Please don't hesitate to open issues or ask for help.

The **Streamline Engine** is a flexible and extensible workflow engine designed to execute complex flows with minimal code. It aims to provide a balance between ease of use and the flexibility to customize and extend functionalities through custom processors.

> **Note:** This project is a **Work In Progress (WIP)**. Both the engine and its tests are under active development. Contributions and feedback are welcome!

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
   - [Installation](#installation)
   - [Basic Usage](#basic-usage)
- [Extending the Engine](#extending-the-engine)
   - [Custom Processors](#custom-processors)
   - [Processor Factories](#processor-factories)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

## Overview

The Streamline Engine allows users to define and execute workflows (flows) composed of various processors. It emphasizes minimal coding effort while providing the flexibility to customize and extend flows with additional processors.

## Features

- **Flexible Workflow Execution**: Define complex workflows with minimal code.
- **Extensibility**: Easily add custom processors to extend the engine's capabilities.
- **Modular Architecture**: Separate repositories for core engine and default processors.
- **Custom Processor Factories**: Wrap existing factories to add custom properties and behaviors.
- **Write-Ahead Logging (WAL)**: Supports recovery and fault tolerance.

## Architecture

The engine is designed with modularity and extensibility in mind:

- **Core Engine**: Manages the execution of flows and processors.
- **Processors**: Individual units of work that can be chained together in a flow.
- **Processor Factory**: Responsible for creating processor instances.
- **Flows**: Definitions of the workflow, including the sequence of processors.

The plan is to develop a separate repository containing default processors and its own processor factory. Users can create their own processor factories to wrap this default factory and add custom properties or processors.

## Getting Started

### Installation

To install the Streamline Engine, you can clone the repository and build it locally:

```bash
git clone https://github.com/yourusername/streamline-engine.git
cd streamline-engine
go build
```

> **Note:** Replace `yourusername` with the actual GitHub username when the repository is available.

### Basic Usage

> **Note**: you can use the [core repository](https://github.com/go-streamline/core) for default implementations for write ahead logger, flow manager and so on.

Here's a basic example of how to use the engine:

```go
package main

import (
    "github.com/go-streamline/engine"
    "github.com/go-streamline/engine/configuration"
    // Import other necessary packages
)

func main() {
    config := &configuration.Config{
        Workdir:           "/path/to/workdir",
        MaxWorkers:        10,
        FlowBatchSize:     5,
        FlowCheckInterval: 5,
    }

    // Initialize other dependencies like write-ahead logger, processor factory, and flow manager
    writeAheadLogger := // Initialize your WAL implementation
    processorFactory := // Initialize your ProcessorFactory implementation
    flowManager :=      // Initialize your FlowManager implementation

    engineInstance, err := engine.New(config, writeAheadLogger, log, processorFactory, flowManager)
    if err != nil {
        // Handle error
    }

    err = engineInstance.Run()
    if err != nil {
        // Handle error
    }

    // Use the engine instance as needed
}
```

## Extending the Engine

### Custom Processors

You can create custom processors by implementing the `Processor` or `TriggerProcessor` interfaces defined in the `definitions` package.

```go
type MyCustomProcessor struct {
    // Implement necessary fields
}

func (p *MyCustomProcessor) Execute(flow *definitions.EngineFlowObject, fileHandler definitions.EngineFileHandler, logger *logrus.Logger) (*definitions.EngineFlowObject, error) {
    // Implement your processor logic
}

func (p *MyCustomProcessor) SetConfig(config map[string]interface{}) error {
    // Handle configuration
}

func (p *MyCustomProcessor) Close() error {
    // Cleanup resources if necessary
}
```

### Processor Factories

You can create your own processor factory to include custom processors:

```go
type MyProcessorFactory struct {
    defaultFactory definitions.ProcessorFactory
}

func (f *MyProcessorFactory) GetProcessor(id uuid.UUID, processorType string) (definitions.Processor, error) {
    // Check if processorType matches your custom processor
    if processorType == "MyCustomProcessor" {
        return &MyCustomProcessor{}, nil
    }
    // Fallback to the default factory
    return f.defaultFactory.GetProcessor(id, processorType)
}
```

## Roadmap

- [ ] Complete core engine features
- [ ] Develop default processors repository
- [ ] Improve documentation and examples
- [ ] Add more comprehensive tests
- [ ] Implement advanced scheduling and error handling features

## Contributing

Contributions are welcome! Please open issues for any bugs or feature requests. When submitting pull requests, ensure that your code adheres to the existing style and includes tests where appropriate.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a pull request

## License

This project is licensed under the terms of the [MIT license](LICENSE).