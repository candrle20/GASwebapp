# Genomic Annotation Service Web App

Built AWS hosted genomic annotation service autoscaling web app in University of Chicago MPCS Cloud Computing


Server Architecture Diagram

![image](https://github.com/user-attachments/assets/3abf4dba-e5b4-4c87-8483-1fb03b5f07c0)


Key Functions
● Log in (via Globus Auth) to use the service -- Some aspects of the service are
available only to registered users. Two classes of users will be supported: Free and
Premium. Premium users will have access to additional functionality, beyond that
available to Free users.
● Upgrade from a Free to a Premium user -- Users will be able to upgrade from Free to
Premium. (Previously this required a mock credit card payments integration, but we have
removed that.)
● Submit an annotation job -- Free users may only submit jobs of up to a certain size.
Premium users may submit any size job. If a Free user submits an oversized job, the
system will refuse it and will prompt the user to convert to a Premium user.
● Receive notifications when annotation jobs finish -- When their annotation request is
complete, the GAS will send users an email that includes a link where they can view the
log file and download the results file.
● Browse jobs and download annotation results -- The GAS will store annotation
results for later retrieval. Users may view a list of their jobs (completed and running).
Free users may download results up to 10 minutes after their job has completed;
thereafter their results will be archived and only available to them if they convert to a
Premium user. Premium users will always have all their data available for download.

System Components
- An object store for input files, annotated (result) files, and job log files.
- A key-value store for persisting information on annotation jobs.
- A low cost, highly-durable object store for archiving the data of Free users.
- A relational database for user account information.
- A service that runs AnnTools for annotation.
- A web application for users to interact with the GAS.
- A set of message queues and notification topics for coordinating system activity.
