# TODO


## Version 1.0.0

* Add a logger as an additional parameter
* Restart unfinished tasks
* Rerun tasks if task failed (use attempts for this)
* Implement task timout. 
  * self._thread.join(timeout=???)
  * What happens when a task has an endless loop? (use timeout for this)
