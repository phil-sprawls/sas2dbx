%macro dedupe(tbl);
  proc sort data=&tbl nodupkey; by id; run;
%mend;
