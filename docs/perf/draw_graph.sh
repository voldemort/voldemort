#!/bin/bash

R --vanilla <<EOF_MARKER
dat <- read.table('mysql_bdb_comparison.dat', header=T)
attach(dat)

pdf('read_comp.pdf')
plot(iteration, mysql_read * 1000, type='l', col='blue', main='BDB vs. MySQL Random Read Performance', xlab='Table Size (Millions)', ylab='Avg. Read Time (microseconds)', ylim=c(0, 450))
lines(iteration, bdb_read * 1000, col='red')
legend('topleft', c('MySQL', 'BDB JE'), col=c('blue', 'red'), lty=c(1,1))
dev.off()

pdf('write_comp.pdf')
plot(iteration, mysql_write * 1000, type='l', col='blue', main='BDB vs. MySQL Random Write Performance', xlab='Table Size (Millions)', ylab='Avg. Write Time (microseconds)', ylim=c(0, 1000))
lines(iteration, bdb_write * 1000, col='red')
legend('topleft', c('MySQL', 'BDB JE'), col=c('blue', 'red'), lty=c(1,1))
dev.off()
EOF_MARKER

convert read_comp.pdf read_comp.png
convert write_comp.pdf write_comp.png