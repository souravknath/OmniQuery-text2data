import { Component, AfterViewChecked, ElementRef, ViewChildren, QueryList, Pipe, PipeTransform } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { ChatService } from './chat.service';
import { marked } from 'marked';
import { Chart, registerables } from 'chart.js/auto';

Chart.register(...registerables);

@Pipe({
  name: 'markdown',
  standalone: true
})
export class MarkdownPipe implements PipeTransform {
  constructor(private sanitizer: DomSanitizer) { }
  transform(value: string): SafeHtml {
    if (!value) return '';
    const html = marked.parse(value) as string;
    return this.sanitizer.bypassSecurityTrustHtml(html);
  }
}

interface ChartData {
  labels: string[];
  datasets: {
    label: string;
    data: number[];
    backgroundColor: string[];
    borderColor: string[];
    borderWidth: number;
  }[];
}

interface Message {
  text: string;
  sender: 'user' | 'agent';
  timestamp: Date;
  hasTable?: boolean;
  showChart?: boolean;
  chartData?: ChartData;
  chartInstance?: Chart | null;
}

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule, MarkdownPipe],
  templateUrl: './app.component.html',
  styleUrl: './app.component.scss'
})
export class AppComponent implements AfterViewChecked {
  title = 'Multi-DB AI Chat';
  userMessage = '';
  messages: Message[] = [
    {
      text: 'Hello! I can help you analyze **Sales, Customers, and Inventory** across multiple databases.',
      sender: 'agent',
      timestamp: new Date()
    }
  ];
  isLoading = false;
  currentStatus = '';

  @ViewChildren('chartCanvas') chartCanvases!: QueryList<ElementRef<HTMLCanvasElement>>;

  constructor(private chatService: ChatService, private sanitizer: DomSanitizer) { }

  ngAfterViewChecked() {
    this.messages.forEach((msg, index) => {
      if (msg.showChart && !msg.chartInstance && msg.chartData) {
        this.initChart(index);
      }
    });
  }



  toggleVisualization(index: number) {
    const msg = this.messages[index];

    if (msg.showChart) {
      if (msg.chartInstance) {
        msg.chartInstance.destroy();
        msg.chartInstance = null;
      }
      msg.showChart = false;
    } else {
      // Try to parse if not already parsed
      if (!msg.chartData) {
        this.parseTableForChart(index);
      }

      // Only show if we actually have data
      if (msg.chartData && msg.chartData.labels.length > 0) {
        msg.showChart = true;
      } else {
        console.warn('Could not generate chart data for this table');
        // Maybe show a temporary toast or just keep the table visible
      }
    }
  }

  parseTableForChart(index: number) {
    const msg = this.messages[index];
    const tokens = marked.lexer(msg.text);

    // Recursive search for table token
    const findTable = (tokenList: any[]): any => {
      for (const t of tokenList) {
        if (t.type === 'table') return t;
        if (t.tokens) {
          const found = findTable(t.tokens);
          if (found) return found;
        }
      }
      return null;
    };

    const tableToken = findTable(tokens);
    if (!tableToken) return;

    const headers = tableToken.header.map((h: any) => h.text);
    const rows = tableToken.rows.map((r: any) => r.map((c: any) => c.text));

    if (rows.length === 0) return;

    // Improved Heuristic: Check all rows to find the best data column
    let labelColIndex = -1;
    let dataColIndex = -1;

    const columnScores = headers.map(() => 0);
    for (const row of rows) {
      row.forEach((cell: string, i: number) => {
        const cleanVal = cell?.replace(/[$,%]/g, '').trim();
        if (cleanVal && !isNaN(parseFloat(cleanVal))) {
          columnScores[i]++;
        }
      });
    }

    // Find the best data column. Prefer right-most columns, and penalize 'ID' columns.
    let bestDataCol = -1;
    for (let i = columnScores.length - 1; i >= 0; i--) {
      if (columnScores[i] > rows.length / 2) {
        const headerLower = (headers[i] || '').toLowerCase();
        const isId = headerLower === 'id' || headerLower.includes(' id') || headerLower.includes('id ') || headerLower === '#';
        if (!isId) {
          bestDataCol = i;
          break;
        }
      }
    }

    if (bestDataCol === -1) {
      // Fallback: just pick any numeric column if no other exists
      bestDataCol = columnScores.findIndex((s: number) => s > rows.length / 2);
    }

    dataColIndex = bestDataCol;

    // Pick first non-numeric column as label
    labelColIndex = columnScores.findIndex((s: number) => s === 0);

    if (dataColIndex === -1) {
      // Last resort: find any column that has at least one number
      dataColIndex = columnScores.findIndex((s: number) => s > 0);
    }

    if (dataColIndex === -1) return; // Still no numeric column
    if (labelColIndex === -1 || labelColIndex === dataColIndex) labelColIndex = 0;

    const labels = rows.map((r: any) => r[labelColIndex]);
    const data = rows.map((r: any) => {
      const cleanVal = r[dataColIndex]?.replace(/[$,%]/g, '').trim();
      return parseFloat(cleanVal) || 0;
    });

    const colors = [
      'rgba(99, 102, 241, 0.7)',  // Indigo
      'rgba(14, 165, 233, 0.7)',  // Sky
      'rgba(168, 85, 247, 0.7)',  // Purple
      'rgba(236, 72, 153, 0.7)',  // Pink
      'rgba(249, 115, 22, 0.7)',  // Orange
      'rgba(34, 197, 94, 0.7)'    // Green
    ];

    msg.chartData = {
      labels,
      datasets: [{
        label: headers[dataColIndex],
        data,
        backgroundColor: labels.map((_: any, i: number) => colors[i % colors.length]),
        borderColor: labels.map((_: any, i: number) => colors[i % colors.length].replace('0.7', '1')),
        borderWidth: 2
      }]
    };
  }

  initChart(index: number) {
    const msg = this.messages[index];
    const canvasRef = this.chartCanvases.toArray().find(c => c.nativeElement.id === `chart-${index}`);

    if (canvasRef && msg.chartData) {
      msg.chartInstance = new Chart(canvasRef.nativeElement, {
        type: 'bar',
        data: msg.chartData!,
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              backgroundColor: 'rgba(255, 255, 255, 0.9)',
              titleColor: '#1e293b',
              bodyColor: '#1e293b',
              borderColor: 'rgba(0,0,0,0.1)',
              borderWidth: 1,
              padding: 10,
              displayColors: true
            }
          },
          scales: {
            y: {
              beginAtZero: true,
              grid: { color: 'rgba(0, 0, 0, 0.05)' },
              ticks: { color: '#64748b', font: { size: 10 } }
            },
            x: {
              grid: { display: false },
              ticks: { color: '#64748b', font: { size: 10 } }
            }
          }
        }
      });
    }
  }

  async sendMessage() {
    if (!this.userMessage.trim() || this.isLoading) return;

    const messageContent = this.userMessage.trim();
    this.messages.push({
      text: messageContent,
      sender: 'user',
      timestamp: new Date()
    });

    this.userMessage = '';
    this.isLoading = true;
    this.currentStatus = '';

    const agentMessage: Message = {
      text: '',
      sender: 'agent',
      timestamp: new Date()
    };
    this.messages.push(agentMessage);

    try {
      const response = await this.chatService.sendMessageStream(messageContent);
      if (!response.body) throw new Error('No response body');

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let leftoverBuffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const chunk = decoder.decode(value, { stream: true });
        const combinedChunk = leftoverBuffer + chunk;
        const lines = combinedChunk.split('\n');
        leftoverBuffer = lines.pop() || '';

        for (const line of lines) {
          if (!line.trim()) continue;
          try {
            const data = JSON.parse(line);
            if (data.type === 'token') {
              agentMessage.text += data.content;
            } else if (data.type === 'phase') {
              this.currentStatus = data.content;
            } else if (data.type === 'tool_start') {
              this.currentStatus = `🔧 Running ${data.tool}...`;
            } else if (data.type === 'tool_end') {
              this.currentStatus = '';
            } else if (data.type === 'error') {
              agentMessage.text = `Error: ${data.content}`;
            }
            this.scrollToBottom();
          } catch (e) { }
        }
      }

      // After streaming is complete, check for tables more accurately
      const tokens = marked.lexer(agentMessage.text);
      const hasTableToken = (tokenList: any[]): boolean => {
        for (const t of tokenList) {
          if (t.type === 'table') return true;
          if (t.tokens && hasTableToken(t.tokens)) return true;
        }
        return false;
      };

      agentMessage.hasTable = hasTableToken(tokens);

    } catch (err) {
      console.error(err);
      agentMessage.text = 'Sorry, I encountered an error connecting to the server.';
    } finally {
      this.isLoading = false;
      this.currentStatus = '';
      this.scrollToBottom();
    }
  }

  scrollToBottom() {
    setTimeout(() => {
      const chatContainer = document.querySelector('.chat-messages');
      if (chatContainer) {
        chatContainer.scrollTop = chatContainer.scrollHeight;
      }
    }, 100);
  }
}

