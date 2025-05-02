// DOM Elements
const repoDirInput = document.getElementById('repo-path');
const validateBtn = document.getElementById('validate-btn');
const repoInfo = document.getElementById('repo-info');
const patternTypeSelect = document.getElementById('pattern-type');
const patternInput = document.getElementById('pattern-input');
const addPatternBtn = document.getElementById('add-pattern-btn');
const excludePatternsEl = document.getElementById('exclude-patterns');
const includePatternsEl = document.getElementById('include-patterns');
const includeSubfoldersEl = document.getElementById('include-subfolders');
const resetExcludeBtn = document.getElementById('reset-exclude-btn');
const clearIncludeBtn = document.getElementById('clear-include-btn');
const excludeRootFilesEl = document.getElementById('exclude-root-files');
const maxFileSizeEl = document.getElementById('max-file-size');
const maxFileSizeValueEl = document.getElementById('max-file-size-value');
const ingestBtn = document.getElementById('ingest-btn');
const tabBtns = document.querySelectorAll('.tab-btn');
const tabPanes = document.querySelectorAll('.tab-pane');
const downloadBtn = document.getElementById('download-btn');
const copyBtn = document.getElementById('copy-btn');
const summaryPlaceholder = document.getElementById('summary-placeholder');
const summaryContent = document.getElementById('summary-content');
const structurePlaceholder = document.getElementById('structure-placeholder');
const structureContent = document.getElementById('structure-content');
const filesPlaceholder = document.getElementById('files-placeholder');
const filesContent = document.getElementById('files-content');
const progressOverlay = document.getElementById('progress-overlay');
const progressBar = document.getElementById('progress-bar');
const progressMessage = document.getElementById('progress-message');
const progressDetails = document.getElementById('progress-details');

// State
let repoDirectory = null;
let directoryStructure = null;
let excludePatterns = [];
let includePatterns = [];
let includeSubfolders = [];
let currentAnalysisJob = null;
let analysisResult = null;

// Initialize
document.addEventListener('DOMContentLoaded', init);

function init() {
    // Load default exclude patterns
    fetchDefaultExcludes();
    
    // Event listeners
    validateBtn.addEventListener('click', validateRepository);
    addPatternBtn.addEventListener('click', addPattern);
    resetExcludeBtn.addEventListener('click', resetExcludePatterns);
    clearIncludeBtn.addEventListener('click', clearIncludePatterns);
    ingestBtn.addEventListener('click', startAnalysis);
    maxFileSizeEl.addEventListener('input', updateMaxFileSizeValue);
    downloadBtn.addEventListener('click', downloadMarkdown);
    copyBtn.addEventListener('click', copyMarkdown);
    
    // Tab switching
    tabBtns.forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });
    
    // Initial UI setup
    updateMaxFileSizeValue();
    switchTab('summary');
}

// API Functions
async function fetchDefaultExcludes() {
    try {
        const response = await fetch('/api/default-excludes');
        const data = await response.json();
        
        excludePatterns = [...data.exclude_patterns];
        renderPatternLists();
    } catch (error) {
        console.error('Error fetching default excludes:', error);
    }
}

async function validateRepository() {
    const path = repoDirInput.value.trim();
    if (!path) return;
    
    try {
        const response = await fetch('/api/validate-directory', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ path })
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Invalid directory');
        }
        
        const data = await response.json();
        repoDirectory = data.full_path;
        
        // Display repository info
        repoInfo.innerHTML = `
            <div class="repo-info-item">
                <span class="repo-info-label">Name:</span>
                <span class="repo-info-value">${data.name}</span>
            </div>
            <div class="repo-info-item">
                <span class="repo-info-label">Path:</span>
                <span class="repo-info-value">${data.full_path}</span>
            </div>
            <div class="repo-info-item">
                <span class="repo-info-label">Files:</span>
                <span class="repo-info-value">${data.approximate_file_count}</span>
            </div>
            <div class="repo-info-item">
                <span class="repo-info-label">Branch:</span>
                <span class="repo-info-value">${data.branch}</span>
            </div>
        `;
        repoInfo.classList.remove('hidden');
        
        // Fetch directory structure
        fetchDirectoryStructure(path);
    } catch (error) {
        alert(`Error: ${error.message}`);
    }
}

async function fetchDirectoryStructure(path) {
    try {
        const response = await fetch('/api/scan-structure', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ path })
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Failed to scan directory');
        }
        
        const data = await response.json();
        directoryStructure = data.directory_structure;
    } catch (error) {
        console.error('Error scanning directory:', error);
    }
}

async function startAnalysis() {
    if (!repoDirectory) {
        alert('Please validate a repository directory first');
        return;
    }
    
    // Prepare analysis options
    const options = {
        directory: repoDirectory,
        exclude_patterns: excludePatterns,
        include_patterns: includePatterns,
        include_subfolders: includeSubfolders,
        exclude_root_files: excludeRootFilesEl.checked,
        max_file_size_kb: parseInt(maxFileSizeEl.value)
    };
    
    // Show progress overlay
    showProgressOverlay('Starting analysis...');
    updateProgress(0.05);
    
    try {
        // Start analysis job
        const response = await fetch('/api/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(options)
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Failed to start analysis');
        }
        
        const data = await response.json();
        currentAnalysisJob = data.job_id;
        
        // Poll for analysis status
        pollAnalysisStatus();
    } catch (error) {
        hideProgressOverlay();
        alert(`Error: ${error.message}`);
    }
}

async function pollAnalysisStatus() {
    if (!currentAnalysisJob) return;
    
    try {
        const response = await fetch(`/api/analysis/${currentAnalysisJob}`);
        if (!response.ok) {
            throw new Error('Failed to get analysis status');
        }
        
        const data = await response.json();
        
        // Update progress
        updateProgress(data.progress);
        
        if (data.status === 'running') {
            // Continue polling
            setTimeout(pollAnalysisStatus, 1000);
        } else if (data.status === 'completed') {
            // Fetch full result
            await fetchAnalysisResult();
        } else if (data.status === 'failed') {
            throw new Error(data.error || 'Analysis failed');
        }
    } catch (error) {
        hideProgressOverlay();
        alert(`Error: ${error.message}`);
    }
}

async function fetchAnalysisResult() {
    if (!currentAnalysisJob) return;
    
    try {
        const response = await fetch(`/api/analysis/${currentAnalysisJob}/full`);
        if (!response.ok) {
            throw new Error('Failed to get analysis result');
        }
        
        // Get the full analysis result
        analysisResult = await response.json();
        
        // Update UI with results
        renderAnalysisResults();
        
        // Enable download and copy buttons
        downloadBtn.disabled = false;
        copyBtn.disabled = false;
        
        // Hide progress overlay
        hideProgressOverlay();
    } catch (error) {
        hideProgressOverlay();
        alert(`Error: ${error.message}`);
    }
}

async function downloadMarkdown() {
    if (!currentAnalysisJob) return;
    
    try {
        const response = await fetch(`/api/analysis/${currentAnalysisJob}/markdown`);
        if (!response.ok) {
            throw new Error('Failed to get markdown content');
        }
        
        const data = await response.json();
        
        // Create a temporary link and download the file
        const blob = new Blob([data.markdown_content], { type: 'text/markdown' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.style.display = 'none';
        a.href = url;
        a.download = `code_ingest_${new Date().toISOString().slice(0, 10)}.md`;
        document.body.appendChild(a);
        a.click();
        
        // Clean up
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
    } catch (error) {
        alert(`Error: ${error.message}`);
    }
}

async function copyMarkdown() {
    if (!currentAnalysisJob) return;
    
    try {
        const response = await fetch(`/api/analysis/${currentAnalysisJob}/markdown`);
        if (!response.ok) {
            throw new Error('Failed to get markdown content');
        }
        
        const data = await response.json();
        
        // Copy to clipboard
        await navigator.clipboard.writeText(data.markdown_content);
        alert('Markdown content copied to clipboard');
    } catch (error) {
        alert(`Error: ${error.message}`);
    }
}

// UI Functions
function addPattern() {
    const pattern = patternInput.value.trim();
    if (!pattern) return;
    
    const type = patternTypeSelect.value;
    
    if (type === 'exclude') {
        excludePatterns.push(pattern);
    } else if (type === 'include') {
        // Check if it should be added to include patterns or subfolders
        if (pattern.endsWith('/') || !pattern.includes('*')) {
            includeSubfolders.push(pattern.replace(/\/$/, ''));
        } else {
            includePatterns.push(pattern);
        }
    }
    
    patternInput.value = '';
    renderPatternLists();
}

function removePattern(type, index) {
    if (type === 'exclude') {
        excludePatterns.splice(index, 1);
    } else if (type === 'include') {
        includePatterns.splice(index, 1);
    } else if (type === 'subfolder') {
        includeSubfolders.splice(index, 1);
    }
    
    renderPatternLists();
}

function resetExcludePatterns() {
    fetchDefaultExcludes();
}

function clearIncludePatterns() {
    includePatterns = [];
    includeSubfolders = [];
    renderPatternLists();
}

function renderPatternLists() {
    // Render exclude patterns
    excludePatternsEl.innerHTML = '';
    excludePatterns.forEach((pattern, index) => {
        const li = document.createElement('li');
        li.innerHTML = `
            <span class="pattern-text">${escapeHtml(pattern)}</span>
            <span class="pattern-remove" title="Remove pattern">
                <i class="fas fa-times"></i>
            </span>
        `;
        li.querySelector('.pattern-remove').addEventListener('click', () => removePattern('exclude', index));
        excludePatternsEl.appendChild(li);
    });
    
    // Render include patterns
    includePatternsEl.innerHTML = '';
    includePatterns.forEach((pattern, index) => {
        const li = document.createElement('li');
        li.innerHTML = `
            <span class="pattern-text">${escapeHtml(pattern)}</span>
            <span class="pattern-remove" title="Remove pattern">
                <i class="fas fa-times"></i>
            </span>
        `;
        li.querySelector('.pattern-remove').addEventListener('click', () => removePattern('include', index));
        includePatternsEl.appendChild(li);
    });
    
    // Render include subfolders
    includeSubfoldersEl.innerHTML = '';
    includeSubfolders.forEach((pattern, index) => {
        const li = document.createElement('li');
        li.innerHTML = `
            <span class="pattern-text">${escapeHtml(pattern)}</span>
            <span class="pattern-remove" title="Remove pattern">
                <i class="fas fa-times"></i>
            </span>
        `;
        li.querySelector('.pattern-remove').addEventListener('click', () => removePattern('subfolder', index));
        includeSubfoldersEl.appendChild(li);
    });
}

function updateMaxFileSizeValue() {
    const value = maxFileSizeEl.value;
    maxFileSizeValueEl.textContent = `${value}kb`;
}

function switchTab(tabId) {
    // Update tab buttons
    tabBtns.forEach(btn => {
        if (btn.dataset.tab === tabId) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });
    
    // Update tab panes
    tabPanes.forEach(pane => {
        if (pane.id === `${tabId}-tab`) {
            pane.classList.add('active');
        } else {
            pane.classList.remove('active');
        }
    });
}

function showProgressOverlay(message) {
    progressMessage.textContent = message || 'Processing...';
    progressBar.style.width = '0%';
    progressOverlay.classList.remove('hidden');
}

function hideProgressOverlay() {
    progressOverlay.classList.add('hidden');
}

function updateProgress(progress) {
    const percent = Math.floor(progress * 100);
    progressBar.style.width = `${percent}%`;
    
    if (progress < 0.2) {
        progressMessage.textContent = 'Starting analysis...';
    } else if (progress < 0.5) {
        progressMessage.textContent = 'Scanning repository...';
    } else if (progress < 0.8) {
        progressMessage.textContent = 'Analyzing files...';
    } else if (progress < 1) {
        progressMessage.textContent = 'Generating report...';
    } else {
        progressMessage.textContent = 'Analysis complete!';
    }
}

function renderAnalysisResults() {
    if (!analysisResult) return;
    
    // Render summary
    renderSummary();
    
    // Render directory structure
    renderDirectoryStructure();
    
    // Render file contents
    renderFileContents();
}

function renderSummary() {
    const summary = analysisResult.summary;
    
    let repoName = '';
    if (Array.isArray(summary.repository)) {
        repoName = summary.repository.map(r => r.split('/').pop()).join(', ');
    } else {
        repoName = summary.repository.split('/').pop();
    }
    
    const tokensInK = (summary.estimated_tokens / 1000).toFixed(1) + 'k';
    
    summaryContent.innerHTML = `
        <div class="summary-box">
            <h2>Repository: ${escapeHtml(repoName)}</h2>
            <div class="summary-stats">
                <div class="stat-item">
                    <div class="stat-value">${summary.files_analyzed}</div>
                    <div class="stat-label">Files analyzed</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">${tokensInK}</div>
                    <div class="stat-label">Estimated tokens</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">${summary.analysis_time.toFixed(2)}s</div>
                    <div class="stat-label">Analysis time</div>
                </div>
            </div>
        </div>
    `;
    
    summaryPlaceholder.classList.add('hidden');
    summaryContent.classList.remove('hidden');
}

function renderDirectoryStructure() {
    const structure = analysisResult.directory_structure;
    const ingestedFiles = analysisResult.ingested_files;
    
    structureContent.innerHTML = `<div class="tree-view"></div>`;
    const treeView = structureContent.querySelector('.tree-view');
    
    renderTreeNode(treeView, structure, '', new Set(ingestedFiles));
    
    structurePlaceholder.classList.add('hidden');
    structureContent.classList.remove('hidden');
}

function renderTreeNode(parent, nodes, currentPath, ingestedFiles) {
    const ul = document.createElement('ul');
    
    nodes.forEach(node => {
        const li = document.createElement('li');
        const nodePath = currentPath ? `${currentPath}/${node.name}` : node.name;
        
        if (node.type === 'directory') {
            li.innerHTML = `
                <div class="tree-item folder">
                    <i class="fas fa-folder"></i>
                    <span>${escapeHtml(node.name)}</span>
                </div>
            `;
            
            // Add children recursively
            if (node.children && node.children.length > 0) {
                renderTreeNode(li, node.children, nodePath, ingestedFiles);
            }
        } else {
            // Check if file was ingested
            const isIngested = ingestedFiles.has(nodePath);
            li.innerHTML = `
                <div class="tree-item file ${isIngested ? 'ingested' : ''}">
                    <i class="fas ${isIngested ? 'fa-file-code' : 'fa-file'}"></i>
                    <span>${escapeHtml(node.name)}</span>
                    ${isIngested ? '<span class="ingested-indicator">✓</span>' : ''}
                </div>
            `;
        }
        
        ul.appendChild(li);
    });
    
    parent.appendChild(ul);
}

function renderFileContents() {
    const fileContents = analysisResult.file_contents;
    
    if (!fileContents || fileContents.length === 0) {
        filesContent.innerHTML = '<div class="no-files-message">No files were ingested</div>';
    } else {
        filesContent.innerHTML = '';
        
        // Display first 5 files
        const filesToShow = fileContents.slice(0, 5);
        
        filesToShow.forEach(file => {
            const fileEl = document.createElement('div');
            fileEl.className = 'file-content';
            
            // Determine file type for syntax highlighting
            const fileExt = file.path.split('.').pop();
            
            fileEl.innerHTML = `
                <div class="file-header">
                    <div class="file-path">${escapeHtml(file.path)}</div>
                    <div class="file-size">${(file.size / 1024).toFixed(1)} KB</div>
                </div>
                <div class="file-body">
                    <pre><code class="language-${fileExt}">${escapeHtml(file.content)}</code></pre>
                </div>
            `;
            
            filesContent.appendChild(fileEl);
        });
        
        // Show message if there are more files
        if (fileContents.length > 5) {
            const moreFiles = document.createElement('div');
            moreFiles.className = 'more-files-message';
            moreFiles.textContent = `${fileContents.length - 5} more files not shown. Download the full report to see all files.`;
            filesContent.appendChild(moreFiles);
        }
    }
    
    filesPlaceholder.classList.add('hidden');
    filesContent.classList.remove('hidden');
}

// Helper Functions
function escapeHtml(unsafe) {
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}