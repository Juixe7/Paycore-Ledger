package com.paycore.payments;

import com.paycore.common.errors.PayCoreException;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Global RFC 7807 {@code application/problem+json} error handler (Section 5.6).
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(PayCoreException.class)
    public ResponseEntity<ProblemDetail> handlePayCoreException(
            PayCoreException ex, HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.valueOf(ex.httpStatus()), ex.getMessage());
        problem.setType(ex.typeUri());
        problem.setTitle(ex.title());
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("errorCode", ex.errorCode());
        return ResponseEntity.status(ex.httpStatus())
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .body(problem);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ProblemDetail> handleIllegalArgument(
            IllegalArgumentException ex, HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.UNPROCESSABLE_ENTITY, ex.getMessage());
        problem.setType(URI.create("https://paycore.dev/problems/invalid-argument"));
        problem.setTitle("Validation Error");
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("errorCode", "VALIDATION_ERROR");
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .body(problem);
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ProblemDetail> handleDataIntegrity(
            DataIntegrityViolationException ex, HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.UNPROCESSABLE_ENTITY, "Database constraint violation: " + ex.getMostSpecificCause().getMessage());
        problem.setType(URI.create("https://paycore.dev/problems/constraint-violation"));
        problem.setTitle("Database Constraint Violation");
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("errorCode", "DATABASE_CONSTRAINT_VIOLATION");
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .body(problem);
    }
}
