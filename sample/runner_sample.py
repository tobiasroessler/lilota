import sys
import time
sys.path.append("../lilota")

from lilota.runner import TaskRunner
from lilota.models import TaskBase
from lilota.stores import MemoryTaskStore, StoreManager

class AdditionTask(TaskBase):

  def run(self):
    self.task_info.output = {
      "result": self.task_info.input["number1"] + self.task_info.input["number2"]
    }
    print(self.output)
    

if __name__ == "__main__":
  StoreManager.register("Store", MemoryTaskStore)
  store_manager = StoreManager()
  store_manager.start()
  store = store_manager.Store()
  runner = TaskRunner(store)
  runner.register("add_task", AdditionTask)
  runner.start()
  runner.add("addition_task", "This task will add number1 to number2 and outputs the result", {"number1": 1, "number2": 2})
  runner.stop()
  time.sleep(30)