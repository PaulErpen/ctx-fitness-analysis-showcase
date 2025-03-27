const gulp = require('gulp');
const sass = require('gulp-sass')(require('sass'));

gulp.task('sass', () => {
    return gulp.src('./scss/**/*.scss')
        .pipe(sass().on('error', sass.logError))
        .pipe(gulp.dest('./ctxdashboard/assets/'));
});

gulp.task('watch', () => {
    gulp.watch('./scss/**/*.scss', (done) => {
        gulp.series(['sass'])(done);
    });
});

gulp.task('default', gulp.series(['sass']));