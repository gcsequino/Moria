# First Pipeline input producer guard profile

The real first producer registered **16/16** candidates in **117.594 seconds** under its unchanged **120-second** deadline. All **67 physical input fixtures** remained configured. The private diagnostic returns after this producer; **no Pipeline acceptance subcase ran**. Its whole command exited zero in 168.676 seconds, and owned controller, labels, scratch pools and temporary paths were verified absent.

The measured hot path made **5,196** Dataset I/O checks and **10,408** filesystem ACL traversals. ACL traversal consumed **35.298 seconds self time**. Accepted-delegation reconstruction consumed another **11.032 seconds self time**. The disposable database recorded **57,735 WAL records**, **3,819,777 logical WAL bytes**, and **14,498 client WAL fsyncs taking 23.371 seconds**. One-second wait samples recorded 62 CPU/no-wait and 25 WalSync samples, with no blockers.

Function statistics, I/O timing and WAL timing were enabled only in the disposable fixture database; counters reset immediately before the producer. Raw samples and function/statistics deltas are retained in profile.json. The profile shows both CPU repetition and durability amplification. A 2.4-second margin does not establish robust acceptance, and this proof changes no guard, deadline, I/O limit, fixture width, or durability setting.
