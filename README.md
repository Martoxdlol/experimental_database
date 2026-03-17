# Experimental Database

## Abstract

This project is an fully working database with some unique design decisions.

Inspired on [postgres](https://www.postgresql.org/) and [Convex](https://www.convex.dev/).

I already made with a lot of AI a working version. But...

1. I don't like not have written almost any code (even if the AI code is good).
2. The AI code isn't actually that good. It has some problems.

I want to build a system I can trust

## Development

Please see [DEVELOPMENT.md](docs/DEVELOPMENT.md) for development guidelines.

## Layers

1. Storage
   1. Pager - manages pages of data on disk
   2. Buffer pool - manages in-memory pages
   3. B+ tree - manages indexes on disk
   4. Write-ahead log - manages durability and crash recovery
   5. Storage engine - encapsulates above components and provides an interface for higher layers
2. Document Store
   1. (wip)