import { CommonModule } from '@angular/common';
import { Component, OnDestroy, OnInit, inject } from '@angular/core';
import { EMPTY, Subject, Subscription, timer } from 'rxjs';
import { catchError, exhaustMap, takeUntil, tap } from 'rxjs/operators';
import { SummaryRow } from './summary.model';
import { SummaryService } from './summary.service';

@Component({
  selector: 'app-root',
  imports: [CommonModule],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App implements OnInit, OnDestroy {
  private readonly summaryService = inject(SummaryService);
  private readonly destroy$ = new Subject<void>();
  private refreshSubscription?: Subscription;

  protected readonly csvUrl = this.summaryService.csvUrl;
  protected rows: SummaryRow[] = [];
  protected loading = true;
  protected error = '';
  protected lastRefresh: Date | null = null;

  ngOnInit(): void {
    this.refreshSubscription = timer(0, 5000)
      .pipe(
        exhaustMap(() => {
          this.loading = this.rows.length === 0;
          return this.summaryService.getSummary().pipe(
            tap((rows) => {
              this.rows = rows;
              this.loading = false;
              this.error = '';
              this.lastRefresh = new Date();
            }),
            catchError(() => {
              this.loading = false;
              this.error = 'Unable to load summary. Retrying automatically.';
              return EMPTY;
            }),
          );
        }),
        takeUntil(this.destroy$),
      )
      .subscribe();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.refreshSubscription?.unsubscribe();
  }
}
