import time
import unittest
from unittest.runner import TextTestResult
SLOW_TEST_THRESHOLD = 3
class TimeLoggingTestResult(TextTestResult):
    def startTest(self, test):
        self._started_at = time.time()
        super().startTest(test)
    def addSuccess(self, test):
        elapsed = time.time() - self._started_at
        if elapsed > SLOW_TEST_THRESHOLD:
            name = self.getDescription(test)
            self.stream.write(
                "\n{} ({:.03}s)\n".format(
                    name, elapsed))
        super().addSuccess(test)
    def addFailure(self, test, err):
        elapsed = time.time() - self._started_at
        if elapsed > SLOW_TEST_THRESHOLD:
            name = self.getDescription(test)
            self.stream.write(
                "\n{} ({:.03}s)\n".format(
                    name, elapsed))
        super().addFailure(test, err)

from unittest import TextTestRunner
if __name__ == '__main__':

    import unittest
    loader = unittest.TestLoader()
    start_dir = '.'
    suite = loader.discover(start_dir)
   
    test_runner = TextTestRunner(resultclass=TimeLoggingTestResult)
    test_runner.run(suite)