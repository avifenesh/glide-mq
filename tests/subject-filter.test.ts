import { describe, it, expect } from 'vitest';
import { matchSubject, compileSubjectMatcher } from '../src/utils';

describe('matchSubject', () => {
  it('exact match', () => {
    expect(matchSubject('projects', 'projects')).toBe(true);
    expect(matchSubject('projects.1', 'projects.1')).toBe(true);
    expect(matchSubject('projects.1.issues.2', 'projects.1.issues.2')).toBe(true);
  });

  it('exact mismatch', () => {
    expect(matchSubject('projects', 'issues')).toBe(false);
    expect(matchSubject('projects.1', 'projects.2')).toBe(false);
    expect(matchSubject('projects.1', 'projects')).toBe(false);
    expect(matchSubject('projects', 'projects.1')).toBe(false);
  });

  it('single-segment wildcard (*)', () => {
    expect(matchSubject('projects.*', 'projects.1')).toBe(true);
    expect(matchSubject('projects.*', 'projects.abc')).toBe(true);
    expect(matchSubject('projects.*', 'projects')).toBe(false);
    expect(matchSubject('projects.*', 'projects.1.issues')).toBe(false);
  });

  it('* in the middle', () => {
    expect(matchSubject('projects.*.issues', 'projects.1.issues')).toBe(true);
    expect(matchSubject('projects.*.issues', 'projects.abc.issues')).toBe(true);
    expect(matchSubject('projects.*.issues', 'projects.1.tasks')).toBe(false);
    expect(matchSubject('projects.*.issues', 'projects.1.issues.2')).toBe(false);
  });

  it('multiple * wildcards', () => {
    expect(matchSubject('*.*.issues', 'projects.1.issues')).toBe(true);
    expect(matchSubject('projects.*.*', 'projects.1.issues')).toBe(true);
    expect(matchSubject('*.*.*', 'a.b.c')).toBe(true);
    expect(matchSubject('*.*.*', 'a.b')).toBe(false);
  });

  it('trailing wildcard (>)', () => {
    expect(matchSubject('projects.>', 'projects.1')).toBe(true);
    expect(matchSubject('projects.>', 'projects.1.issues')).toBe(true);
    expect(matchSubject('projects.>', 'projects.1.issues.2')).toBe(true);
    expect(matchSubject('projects.>', 'projects')).toBe(false);
    expect(matchSubject('>', 'anything')).toBe(true);
    expect(matchSubject('>', 'a.b.c')).toBe(true);
  });

  it('> with prefix segments', () => {
    expect(matchSubject('projects.*.issues.>', 'projects.1.issues.2')).toBe(true);
    expect(matchSubject('projects.*.issues.>', 'projects.1.issues.2.comments')).toBe(true);
    expect(matchSubject('projects.*.issues.>', 'projects.1.tasks.2')).toBe(false);
    expect(matchSubject('projects.*.issues.>', 'projects.1.issues')).toBe(false);
  });
  it('> must be the last token', () => {
    expect(() => matchSubject('projects.>.issues', 'projects.1.issues')).toThrow(
      '`>` wildcard must be the last token',
    );
  });
});

describe('compileSubjectMatcher', () => {
  it('returns null for undefined or empty patterns', () => {
    expect(compileSubjectMatcher(undefined)).toBe(null);
    expect(compileSubjectMatcher([])).toBe(null);
  });

  it('single pattern', () => {
    const matcher = compileSubjectMatcher(['projects.*'])!;
    expect(matcher('projects.1')).toBe(true);
    expect(matcher('projects.1.issues')).toBe(false);
  });

  it('multiple patterns (OR logic)', () => {
    const matcher = compileSubjectMatcher(['projects.*', 'issues.>'])!;
    expect(matcher('projects.1')).toBe(true);
    expect(matcher('issues.42')).toBe(true);
    expect(matcher('issues.42.comments')).toBe(true);
    expect(matcher('tasks.1')).toBe(false);
  });

  it('matches exact subjects in multi-pattern list', () => {
    const matcher = compileSubjectMatcher(['orders.created', 'orders.updated'])!;
    expect(matcher('orders.created')).toBe(true);
    expect(matcher('orders.updated')).toBe(true);
    expect(matcher('orders.deleted')).toBe(false);
  });
});
