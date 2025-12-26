// JavaScript for timeline ruler interaction (Wayback Machine style).
(function() {
    const data = window.TIMELINE_DATA;
    if (!data || !data.buckets || data.buckets.length === 0) return;

    const rulerLabels = document.getElementById('ruler-labels');
    const rulerSelection = document.getElementById('ruler-selection');
    const startRange = document.getElementById('start-range');
    const endRange = document.getElementById('end-range');
    const dateRangeDisplay = document.getElementById('date-range');
    const docCountDisplay = document.getElementById('doc-count');
    const resetBtn = document.getElementById('reset-timeline');
    const table = document.getElementById('document-table');

    // Find min/max dates and max count
    const dates = data.buckets.map(b => new Date(b.date));
    const minDate = new Date(Math.min(...dates));
    const maxDate = new Date(Math.max(...dates));
    const maxCount = Math.max(...data.buckets.map(b => b.count));
    const totalDocs = data.total;

    // Build a map of date -> count for quick lookup
    const dateCountMap = {};
    data.buckets.forEach(b => { dateCountMap[b.date] = b.count; });

    // Generate ruler ticks - show years as major, months with activity as minor
    function buildRuler() {
        rulerLabels.innerHTML = '';

        const startYear = minDate.getFullYear();
        const endYear = maxDate.getFullYear();
        const totalMs = maxDate - minDate;

        // If span is less than 2 years, show months; otherwise show years
        const showMonths = (endYear - startYear) <= 2;

        if (showMonths) {
            // Show each month
            let current = new Date(minDate.getFullYear(), minDate.getMonth(), 1);
            const end = new Date(maxDate.getFullYear(), maxDate.getMonth() + 1, 1);

            while (current <= end) {
                const pos = totalMs > 0 ? ((current - minDate) / totalMs) * 100 : 0;
                const isJan = current.getMonth() === 0;
                const label = isJan
                    ? current.getFullYear().toString()
                    : current.toLocaleString('default', { month: 'short' });

                // Count docs in this month
                const monthKey = current.toISOString().slice(0, 7);
                const monthCount = data.buckets
                    .filter(b => b.date.startsWith(monthKey))
                    .reduce((sum, b) => sum + b.count, 0);

                createTick(pos, label, isJan ? 'major' : 'minor', monthCount, current.getTime());

                current.setMonth(current.getMonth() + 1);
            }
        } else {
            // Show years
            for (let year = startYear; year <= endYear; year++) {
                const yearStart = new Date(year, 0, 1);
                const pos = totalMs > 0 ? ((yearStart - minDate) / totalMs) * 100 : 0;

                // Count docs in this year
                const yearCount = data.buckets
                    .filter(b => b.date.startsWith(year.toString()))
                    .reduce((sum, b) => sum + b.count, 0);

                createTick(Math.max(0, Math.min(100, pos)), year.toString(), 'major', yearCount, yearStart.getTime());
            }
        }

        // Add end cap
        createTick(100, '', 'minor', 0, maxDate.getTime());
    }

    function createTick(position, label, type, count, timestamp) {
        const tick = document.createElement('div');
        tick.className = `ruler-tick ${type}`;
        tick.style.left = `${position}%`;
        tick.dataset.timestamp = timestamp;

        // Density indicator based on document count
        if (count > 0) {
            const density = document.createElement('div');
            density.className = 'density';
            if (count >= maxCount * 0.7) {
                density.classList.add('high');
            } else if (count >= maxCount * 0.3) {
                density.classList.add('medium');
            }
            density.title = `${count} documents`;
            tick.appendChild(density);
        }

        const mark = document.createElement('div');
        mark.className = 'tick-mark';
        tick.appendChild(mark);

        if (label) {
            const labelEl = document.createElement('div');
            labelEl.className = 'tick-label';
            labelEl.textContent = label;
            tick.appendChild(labelEl);
        }

        rulerLabels.appendChild(tick);
    }

    // Update selection highlight on ruler
    function updateRulerSelection() {
        const startPct = parseFloat(startRange.value);
        const endPct = parseFloat(endRange.value);
        rulerSelection.style.left = `${startPct}%`;
        rulerSelection.style.width = `${endPct - startPct}%`;
    }

    // Filter function
    function filterByDateRange() {
        const startPct = parseFloat(startRange.value) / 100;
        const endPct = parseFloat(endRange.value) / 100;

        const totalMs = maxDate - minDate;
        const startTs = minDate.getTime() + (totalMs * startPct);
        const endTs = minDate.getTime() + (totalMs * endPct);

        const startDate = new Date(startTs);
        const endDate = new Date(endTs);

        // Update display
        const formatDate = d => d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
        dateRangeDisplay.textContent = `${formatDate(startDate)} — ${formatDate(endDate)}`;

        // Count visible docs
        let visibleCount = 0;

        // Filter table rows
        if (table) {
            const rows = table.querySelectorAll('tbody tr');
            rows.forEach(row => {
                const rowTs = parseInt(row.dataset.date, 10) * 1000;
                if (rowTs >= startTs && rowTs <= endTs) {
                    row.classList.remove('hidden');
                    visibleCount++;
                } else {
                    row.classList.add('hidden');
                }
            });
        }

        docCountDisplay.textContent = `(${visibleCount} of ${totalDocs} docs)`;

        // Update ruler selection highlight
        updateRulerSelection();

        // Update tick active states
        const ticks = rulerLabels.querySelectorAll('.ruler-tick');
        ticks.forEach(tick => {
            const tickTs = parseInt(tick.dataset.timestamp, 10);
            if (tickTs >= startTs && tickTs <= endTs) {
                tick.classList.add('active');
            } else {
                tick.classList.remove('active');
            }
        });
    }

    startRange.addEventListener('input', filterByDateRange);
    endRange.addEventListener('input', filterByDateRange);

    resetBtn.addEventListener('click', () => {
        startRange.value = 0;
        endRange.value = 100;
        filterByDateRange();
    });

    // Build the ruler and initialize
    buildRuler();
    docCountDisplay.textContent = `(${totalDocs} docs)`;
    updateRulerSelection();
})();
