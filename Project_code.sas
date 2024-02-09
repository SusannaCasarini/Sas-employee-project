/*Import the single datasets on SAS*/
libname data "C:\Users\susanna.casarini\Downloads\Data\Data";

/*DATA step to create three datasets*/
data addresses;
      set data.employee_addresses;
run;

data donations;
      set data.employee_donations;
run;

data organization;
      set data.employee_organization;
run;

/*PROC CONTENTS step to study the datasets*/
proc contents data=addresses;
run;

proc contents data=donations;
run;

proc contents data=organization;
run;

/*PROC SORT step to prepare the datasets for the merge*/
proc sort data=addresses;
      by Employee_ID;
run;

proc sort data=donations;
      by Employee_ID;
      
run;proc sort data=organization;
      by Employee_ID;
run;

/*DATA step with MERGE statements*/
data employee1;
      merge addresses(in=add)
              donations(in=don)
              organization(in=org);         
      by Employee_ID;
      if add=1 AND (qtr1>0 or qtr2>0 or qtr3>0);
run;

/*CATX statement to create the address variable*/
data employee2;
      set work.employee1;
      address = catx(", ",street_number,street_name,city,country);
run;

/*PROC REPORT using ODS language to create a first report*/
ods rtf file='C:\Users\susanna.casarini\Downloads\Data\Data\Employees_First_Report.rtf';
proc report data=employee2;
      columns Employee_ID Department Address;
      define employee_ID / display 'Employee ID';
      define department / display 'Department' ;
      define address / display 'Address' ;
      endcomp;
run;
ods rtf close;

/*Create a second  report in which employees are grouped by department  (ex. Administration)
and job role (ex. Director). Print also the variable DON_MEAN,
that contains the mean of donations made in the specific periods (qtr1-qtr4),
DON_MAX that contains the maximum donation made
and DON_MIN with the minimum one*/

/*DATA step to create the three variable: mean min max for the donations in the four periods*/
data employee3;
      set work.employee2;
      don_mean = mean(qtr1, qtr2, qtr3, qtr4);
      don_max = max(qtr1, qtr2, qtr3, qtr4);
      don_min = min(qtr1, qtr2, qtr3, qtr4);
run;

ods rtf file='C:\Users\susanna.casarini\Downloads\Data\Data\Employees_Second_Report.rtf';
proc report data=employee3;
            columns Employee_ID Department Job_title don_mean don_max don_min;
            define employee_ID / display 'Employee ID';
            define department / order 'Department' ;
            define Job_title / order 'Job Title' ;
            define don_mean / display 'Don_Mean' ;
            define don_max / display 'Don_Max' ;
            define don_min / display 'Don_Min' ;
      endcomp;
run;
ods rtf close;

/*Finally, create a table that shows the mean of the donation
made from qtr1 to qtr4 by department,
as shown in the following table: ...*/
/*Use proper variable to print inside a report*/
      ods rtf file='C:\Users\susanna.casarini\Downloads\Data\Data\Employees_Table.rtf';
      proc means data=employee3 min max mean noobs;
            var qtr1 qtr2 qtr3 qtr4 ;
            class department ;
            output out= employee_dep;
      run;
      ods rtf close;

      proc sort data=employee_dep out=employee_sort;
      by department;
      run;
      
      proc transpose data=employee_sort out=employee_transp name=period;
      var qtr1 qtr2 qtr3 qtr4 ;
      by department;
      run;

      proc sort data=employee_transp out=transp2;
      by department period;
      run;
      
      proc contents data=transp2;
      run;

      ods rtf file='C:\Users\susanna.casarini\Downloads\Data\Data\Employees_Third_Report.rtf';
      proc report data=transp2;
            columns department period col2-col4;
            define department / order 'Department';
            define period / order 'Period' ;
            define col2 / display 'Minimum';
            define col3 / display 'Maximum';
            define col4 / display 'Mean';
            endcomp;
      run;
      ods rtf close;
      

%macro report(class_used=) ;

      ods rtf file='C:\Users\susanna.casarini\Downloads\Data\Data\Employees_Fourth_Report.rtf';
      proc means data=employee_job min max mean noobs;
            var qtr1 qtr2 qtr3 qtr4 ;
            class &class_used ;
            output out= employee_job;
      run;
      ods rtf close;

      proc sort data=employee_job out=employee_sort_job;
      by &class_used;
      run;
      
      proc transpose data=employee_sort_job out=employee_transp_job name=period;
      var qtr1 qtr2 qtr3 qtr4 ;
      by &class_used;
      run;

      proc sort data=employee_transp_job out=transp2_job;
      by &class_used period;
      run;
      
      proc contents data=transp2_job;
      run;

      ods rtf file='C:\Users\susanna.casarini\Downloads\Data\Data\Employees_Fifth_Report.rtf';
      proc report data=transp2_job;
            columns &class_used period col2-col4;
            define &class_used / order '&class_used';
            define period / order 'Period' ;
            define col2 / display 'Minimum';
            define col3 / display 'Maximum';
            define col4 / display 'Mean';
            endcomp;
      run;
      ods rtf close;
      
%mend report;

%report(class_used=department);

/*Repeat the same operation grouping by job role (use a macro to improve efficiency)*/
%report(class_used=job_title);


/*Create the variable COUNT_DON that counts the number of donations made.
If the total number of donations is greater than 1 and
the total amount exceed 30$, then categorize the subject as benefactor
(create a flag variable BNFL that assumes value "Y" if the subject is a benefactor and "N" otherwise)*/

data employee_count;
      set employee3;
      count_don = 4-(sum(qtr1=0, qtr2=0, qtr3=0, qtr4=0));
      if count_don > 0 AND (qtr1+qtr2+qtr3+qtr4) > 30  then BNFL="Y";
      else BNFL="N";
run;

/*Which role is associated to being benefactor?*/
proc freq data=employee_count;
	table job_title*BNFL / nopercent norow nocol;
run;

proc freq data=employee_count;
	table department*BNFL / nopercent norow nocol;
run;