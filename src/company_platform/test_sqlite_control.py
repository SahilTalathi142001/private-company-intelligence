
import tempfile, threading, unittest, os, sys, time
sys.path.insert(0,os.path.dirname(__file__))
from sqlite_control import SQLiteControlStore, LostClaim
class StoreTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(); self.s=SQLiteControlStore(os.path.join(self.tmp.name,"c.db"),lease_seconds=1)
 def tearDown(self): self.tmp.cleanup()
 def test_claim_single_winner(self):
  self.s.put_event({"event_id":"e1","event_type":"X"})
  out=[]
  def f(i): out.append(self.s.claim("e1",str(i)))
  ts=[threading.Thread(target=f,args=(i,)) for i in range(12)]
  [t.start() for t in ts]; [t.join() for t in ts]
  self.assertEqual(sum(x is not None for x in out),1)
 def test_expired_lease_fences_old_worker(self):
  self.s.put_event({"event_id":"e2"})
  a=self.s.claim("e2","a"); time.sleep(1.05); b=self.s.claim("e2","b")
  self.assertIsNotNone(b)
  with self.assertRaises(LostClaim): self.s.finish_event("e2",a["token"],"COMPLETED")
  self.s.finish_event("e2",b["token"],"COMPLETED")
 def test_atomic_idempotent_run(self):
  run={"run_id":"r1","trigger_event_id":"e3","status":"PENDING"}
  tasks=[{"task_id":"t1","component_id":"x"},{"task_id":"t2","component_id":"y"}]
  self.assertEqual(self.s.create_run_once(run,tasks),("r1",True))
  self.assertEqual(self.s.create_run_once({"run_id":"r2","trigger_event_id":"e3"},[]),("r1",False))
  self.assertEqual(self.s.task_count("r1"),2)
 def test_failed_insert_rolls_back_everything(self):
  with self.assertRaises(Exception):
   self.s.create_run_once({"run_id":"r4","trigger_event_id":"e4"},[{"task_id":"t","component_id":"x"},{"task_id":"t","component_id":"y"}])
  self.assertIsNone(self.s.run_for_event("e4"))
if __name__=="__main__": unittest.main()
