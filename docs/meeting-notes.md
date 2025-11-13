# Meeting Notes

## October 23, 2024

- Created [a work plan](./plan.md)
- Discussed creation of station codes list and how to determine what data is missing from the datasets
- Postponed greater architecture discussions until after PA03 has been completed

## October 30, 2024

- Discussed whether or not to use MapReduce or Spark
  - Most of the team felt more comfortable in MapReduce, so we will select that
- Mike uploaded an initial sample of some java files and a Makefile
- There was concern about the effect of removing a column from the dataset and how that could effect the sorting
  - Sorting is necessary for the composite join pattern
- Some of the files have large gaps in the years
  - For example, `kacap` has a file from 1944 then jumps to 2014
  - We want to only take contiguous years, if possible
- Some stations didn't start until 2022 or 2023
  - We should skip those stations

![files with year gaps](https://github.com/user-attachments/assets/e51554d5-4c3e-4f28-a117-12c4a0a4c28f)


## December 2nd, 2024

- Discussed and worked on presentation drafting for Wednesday's presentation.
- Suggested for Mike and Owen if wanted to record a video for their slides. It could be like an audio recording or video, this was based on what Eryn and Sofia saw on Monday's presentation
- Mentioned that for Friday's demo we could possibly share screen our Github repo and everyone talks about their parts
  - We're assuming the demo would be similar to PA's but more presentation style with Dr. Paliickara asking questions in-between or after
