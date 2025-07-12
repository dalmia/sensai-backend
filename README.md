# SensAI backend

[![codecov](https://codecov.io/gh/dalmia/sensai-backend/branch/main/graph/badge.svg)](https://codecov.io/gh/dalmia/sensai-backend)

SensAI is an AI-first Learning Management System (LMS) which enables educators to help them teacher smarter and reach further. SensAI coaches your students through questions that develop deeper thinking—just like you would, but for every student and all the time. This repository is the backend for SensAI. The frontend repository can be found [here](https://github.com/dalmia/sensai-frontend).

If you are using SensAI and have any feedback for us or want any help with using SensAI, please consider [joining our community](https://chat.whatsapp.com/LmiulDbWpcXIgqNK6fZyxe) of AI + Education builders and reaching out to us.

If you want to contribute to SensAI, please look at the `Contributing` section below.

Our public roadmap is live [here](https://hyperverge.notion.site/fa1dd0cef7194fa9bf95c28820dca57f?v=ec52c6a716e94df180dcc8ced3d87610). Go check it out and let us know what you think we should build next!

## Contributing
To learn more about making a contribution to SensAI, please see our [Contributing guide](./docs/CONTRIBUTING.md).

## Installation
Refer to the [INSTALL.md](./docs/INSTALL.md) file for instructions on how to install and run the backend locally.

## Testing
SensAI uses pytest for testing the API endpoints and measuring code coverage. To run the tests and generate coverage reports, follow these instructions:

### Installing Test Dependencies
```bash
pip install -r requirements-dev.txt
```

### Running Tests
To run all tests and generate a coverage report:
```bash
./run_tests.sh
```

### Coverage Reports
After running the full test suite with `run_tests.sh`, a HTML coverage report will be generated in the `coverage_html` directory. Open `coverage_html/index.html` in your browser to view the report.

### Codecov Integration
This project is integrated with [Codecov](https://codecov.io) for continuous monitoring of code coverage. Coverage reports are automatically generated and uploaded to Codecov when a PR is made to the `main` branch or when a commit is made to the `main` branch. The Codecov badge at the top of this README shows the current coverage status.

## Community
We are building a community of creators, builders, teachers, learners, parents, entrepreneurs, non-profits and volunteers who are excited about the future of AI and education. If you identify as one and want to be part of it, consider [joining our community](https://chat.whatsapp.com/LmiulDbWpcXIgqNK6fZyxe).