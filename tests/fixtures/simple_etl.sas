options nodate;
libname mylib '/data/mylib';
%let cutoff = 2024-01-01;

data work.filtered;
  set mylib.customers;
  where signup_date >= "&cutoff."d;
run;

proc sql;
  create table work.summary as
  select region, count(*) as n
  from work.filtered
  group by region;
quit;

proc means data=work.filtered noprint;
  var balance;
  output out=work.stats mean=avg_balance;
run;
