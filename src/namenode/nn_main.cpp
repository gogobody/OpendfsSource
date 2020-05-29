#include <dirent.h>
#include "nn_main.h"
#include "../../etc/config.h"
#include "dfs_conf.h"
#include "nn_cycle.h"
#include "nn_signal.h"
#include "nn_module.h"
#include "nn_process.h"
#include "nn_conf.h"
#include "nn_time.h"

#define DEFAULT_CONF_FILE PREFIX "/etc/namenode.conf"

#define PATH_LEN  256

int          dfs_argc;
char       **dfs_argv;

string_t     config_file;
static int   g_format = NGX_FALSE;
static int   test_conf = NGX_FALSE;
static int   g_reconf = NGX_FALSE;
static int   g_quit = NGX_FALSE;
static int   show_version;
sys_info_t   dfs_sys_info;
extern pid_t process_pid;

static int parse_cmdline(int argc, char *const *argv);
static int conf_syntax_test(cycle_t *cycle);
static int sys_set_limit(uint32_t file_limit, uint64_t mem_size);
static int sys_limit_init(cycle_t *cycle);
static int format(cycle_t *cycle);
static int clear_current_dir(cycle_t *cycle);
static int save_ns_version(cycle_t *cycle);
static int get_ns_version(cycle_t *cycle);

static void dfs_show_help(void)
{
    printf("\t -c, Configure file\n"
		"\t -f, format the DFS filesystem\n"
        "\t -v, Version\n"
        "\t -t, Test configure\n"
        "\t -r, Reload configure file\n"
        "\t -q, stop namenode server\n");

    return;
}

static int parse_cmdline( int argc, char *const *argv)
{
    char ch = 0;
    char buf[255] = {0};

    while ((ch = getopt(argc, argv, "c:fvtrqhV")) != -1) 
	{
        switch (ch) 
		{
            case 'c':
                if (!optarg) 
				{
                    dfs_show_help();
					
                    return NGX_ERROR;
                }
				
                if (*optarg == '/') 
				{
                    config_file.data = (uchar_t  *)strdup(optarg);
                    config_file.len = strlen(optarg);
                } 
				else 
				{
                    getcwd(buf,sizeof(buf));
                    buf[strlen(buf)] = '/';
                    strcat(buf,optarg);
                    config_file.data = (uchar_t  *)strdup(buf);
                    config_file.len = strlen(buf);
                }
				
                break;

			case 'f':
				g_format = NGX_TRUE;
				break;
				
            case 'V':
            case 'v':
                printf("namenode version: \"PACKAGE_STRING\"\n");
#ifdef CONFIGURE_OPTIONS
                printf("configure option:\"CONFIGURE_OPTIONS\"\n");
#endif
                show_version = 1;

                exit(0);
				
                break;
				
            case 't':
                test_conf = NGX_TRUE;
                break;
				
            case 'r':
                g_reconf= NGX_TRUE;
                break;
				
            case 'q':
                g_quit = NGX_TRUE;
                break;
				
            case 'h':
				
            default:
                dfs_show_help();
				
                return NGX_ERROR;
        }
    }

    return NGX_OK;
}

int main(int argc, char **argv)
{
    int            ret = NGX_OK;
    cycle_t       *cycle = nullptr;
    conf_server_t *sconf = nullptr;
    
    cycle = cycle_create();

    time_init();

    if (parse_cmdline(argc, argv) != NGX_OK)
	{
        return NGX_ERROR;
    }

    if (!show_version && sys_get_info(&dfs_sys_info) != NGX_OK)
	{
        return NGX_ERROR;
    }
 
    if (config_file.data == nullptr)
	{
        config_file.data = (uchar_t *)strndup(DEFAULT_CONF_FILE,
            strlen(DEFAULT_CONF_FILE));
        config_file.len = strlen(DEFAULT_CONF_FILE);
    }
    
    if (test_conf == NGX_TRUE)
	{
        ret = conf_syntax_test(cycle);
		
        goto out;
    }
    
    if (g_reconf || g_quit) 
	{
        if ((ret = cycle_init(cycle))!= NGX_OK)
		{
            fprintf(stderr, "cycle_init fail\n");
			
            goto out;
        }
		
        process_pid = process_get_pid(cycle);
        if (process_pid < 0)
		{
            fprintf(stderr, " get server pid fail\n");
            ret = NGX_ERROR;
			
            goto out;
        }

        if (g_reconf) 
		{
            kill(process_pid, SIGNAL_RECONF);
            printf("config is reloaded\n");
        }
		else 
		{
            kill(process_pid, SIGNAL_QUIT);
            printf("service is stoped\n");
        }
		
        ret = NGX_OK;
		
        goto out;
    }

    umask(0022);
    // init conf 
    if ((ret = cycle_init(cycle)) != NGX_OK)
	{
        fprintf(stderr, "cycle_init fail\n");
		
        goto out;
    }
    // format the file system
    // init namespace id
	if (g_format) 
	{
	    format(cycle);
		
        goto out;
	}

    if ((ret = process_check_running(cycle)) == NGX_TRUE)
	{
        fprintf(stderr, "namenode is already running\n");
		
    	goto out;
    }

    if ((ret = sys_limit_init(cycle)) != NGX_OK)
	{
        fprintf(stderr, "sys_limit_init error\n");
		
    	goto out;
    }
    
	ngx_module_setup();
    // just nn_error_log_init
	if ((ret = ngx_module_master_init(cycle)) != NGX_OK)
	{
		fprintf(stderr, "master init fail\n");
		
        goto out;
	}
    
    sconf = (conf_server_t *)cycle->sconf;

    if (process_change_workdir(&sconf->coredump_dir) != NGX_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
			"process_change_workdir failed!");
		
        goto failed;
    }

    if (signal_setup() != NGX_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
			"setup signal failed!");
		
        goto failed;
    }
    // 守护进程
//    if (sconf->daemon == NGX_TRUE && nn_daemon() == NGX_ERROR)
//	{
//        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
//			"dfs_daemon failed");
//
//        goto failed;
//    }

    process_pid = getpid();
	
    if (process_write_pid_file(process_pid) == NGX_ERROR)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
			"write pid file error");
		
        goto failed;
    }
    // namespaceid is dfs_current_msec
	if (get_ns_version(cycle) != NGX_OK)
	{
	    dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
			"the DFS filesystem is unformatted");
		
        goto failed;
	}

    dfs_argc = argc;
    dfs_argv = argv;

	
    //
    ngx_master_process_cycle(cycle, dfs_argc, dfs_argv);

    process_del_pid_file();

failed:
    ngx_module_master_release(cycle);

out:
    if (config_file.data) 
	{
        free(config_file.data);
        config_file.len = 0;
    }
	
    if (cycle) 
	{
        cycle_free(cycle);
    }

    return ret;
}

int nn_daemon()
{
    int fd = NGX_INVALID_FILE;
    int pid = NGX_ERROR;
	
    pid = fork();

    if (pid > 0) 
	{
        exit(0);
    } 
	else if (pid < 0) 
	{
        printf("dfs_daemon: fork failed\n");
		
        return NGX_ERROR;
    }
	
    if (setsid() == NGX_ERROR)
	{
        printf("dfs_daemon: setsid failed\n");
		
        return NGX_ERROR;
    }

    umask(0022);

    fd = open("/dev/null", O_RDWR);
    if (fd == NGX_INVALID_FILE)
	{
        return NGX_ERROR;
    }

    if (dup2(fd, STDIN_FILENO) == NGX_ERROR)
	{
        printf("dfs_daemon: dup2(STDIN) failed\n");
		
        return NGX_INVALID_FILE;
    }

    if (dup2(fd, STDOUT_FILENO) == NGX_ERROR)
	{
        printf("dfs_daemon: dup2(STDOUT) failed\n");
		
        return NGX_ERROR;
    }

    if (dup2(fd, STDERR_FILENO) == NGX_ERROR)
	{
        printf("dfs_daemon: dup2(STDERR) failed\n");
		
        return NGX_ERROR;
    }

    if (fd > STDERR_FILENO) 
	{
        if (close(fd) == NGX_ERROR)
		{
            printf("dfs_daemon: close() failed\n");
			
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

static int conf_syntax_test(cycle_t *cycle)
{
    if (config_file.data == nullptr)
	{
        return NGX_ERROR;
    }

    if (cycle_init(cycle) == NGX_ERROR)
	{
        return NGX_ERROR;
    }
	
    return NGX_OK;
}

static int sys_set_limit(uint32_t file_limit, uint64_t mem_size)
{
    int            ret = NGX_ERROR;
    int            need_set = NGX_FALSE;
    struct rlimit  rl;
    log_t         *log;

    log = dfs_cycle->error_log;

    ret = getrlimit(RLIMIT_NOFILE, &rl);
    if (ret == NGX_ERROR)
	{
        dfs_log_error(log, DFS_LOG_ERROR,
            errno, "sys_set_limit get RLIMIT_NOFILE error");
		
        return ret;
    }
	
    if (rl.rlim_max < file_limit) 
	{
        rl.rlim_max = file_limit;
        need_set = NGX_TRUE;
    }
	
    if (rl.rlim_cur < file_limit) 
	{
        rl.rlim_cur = file_limit;
        need_set = NGX_TRUE;
    }
	
    if (need_set) 
	{
        ret = setrlimit(RLIMIT_NOFILE, &rl);
        if (ret == NGX_ERROR)
		{
            dfs_log_error(log, DFS_LOG_ERROR,
                errno, "sys_set_limit set RLIMIT_NOFILE error");
			
            return ret;
        }
    }

    // set mm overcommit policy to use large block memory
    if (mem_size > ((size_t)1 << 32)) 
	{
        ret = system("sysctl -w vm.overcommit_memory=1 > /dev/zero");
        if (ret == NGX_ERROR)
		{
            dfs_log_error(log, DFS_LOG_ERROR,
                errno, "sys_set_limit set vm.overcommit error");
			
            return NGX_ERROR;
        }
    }

    // enable core dump
    if (prctl(PR_SET_DUMPABLE, 1, 0, 0, 0) != 0) 
	{
        dfs_log_error(log, DFS_LOG_ERROR,
            errno, "sys_set_limit set PR_SET_DUMPABLE error");
		
        return NGX_ERROR;
    }
	
    if (getrlimit(RLIMIT_CORE, &rl) == 0) 
	{
        rl.rlim_cur = rl.rlim_max;
		
        ret = setrlimit(RLIMIT_CORE, &rl);
        if (ret == NGX_ERROR)
		{
            dfs_log_error(log, DFS_LOG_ERROR,
                errno, "sys_set_limit set RLIMIT_CORE error");
			
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

static int sys_limit_init(cycle_t *cycle)
{
    conf_server_t *sconf = (conf_server_t *)cycle->sconf;
    
    return sys_set_limit(sconf->connection_n, 0);
}

// format the filesystem 
static int format(cycle_t *cycle)
{
    conf_server_t *sconf = (conf_server_t *)cycle->sconf;

	fprintf(stdout, "Re-format filesystem in %s ? (Y or N)\n", 
		sconf->fsimage_dir.data);

	char input;
	scanf("%c", &input);

	if (input == 'Y') 
	{
		if (clear_current_dir(cycle) != NGX_OK)
		{
            return NGX_ERROR;
		}

		// namespace id is dfs_current_msec
		if (save_ns_version(cycle) != NGX_OK)
		{
            return NGX_ERROR;
		}

		fprintf(stdout, "Storage directory %s has been successfully formatted.\n", 
		    sconf->fsimage_dir.data);
	}
	else 
	{
        fprintf(stdout, "Format aborted in %s\n", sconf->fsimage_dir.data);
	}
	
    return NGX_OK;
}


// format 1
static int clear_current_dir(cycle_t *cycle)
{
    conf_server_t *sconf = (conf_server_t *)cycle->sconf;
	
    char dir[PATH_LEN] = {0};
	string_xxsprintf((uchar_t *)dir, "%s/current", sconf->fsimage_dir.data);

	if (access(dir, F_OK) != NGX_OK)
	{
	    if (mkdir(dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH) != NGX_OK)
	    {
	        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
				"mkdir %s err", dir);
		
	        return NGX_ERROR;
	    }

		return NGX_OK;
	}
	
	DIR *dp = nullptr;
    struct dirent *entry = nullptr;
	
    if ((dp = opendir(dir)) == nullptr)
	{
        printf("open %s err: %s\n", dir, strerror(errno));
		
        return NGX_ERROR;
    }

	while ((entry = readdir(dp)) != nullptr)
	{
		if (entry->d_type == 8)
		{
		    // file
            char sBuf[PATH_LEN] = {0};
 		    sprintf(sBuf, "%s/%s", dir, entry->d_name);
 		    printf("unlink %s\n", sBuf);
 
 		    unlink(sBuf);
		}
		else if (0 != strcmp(entry->d_name, ".") 
			&& 0 != strcmp(entry->d_name, ".."))
		{
            // sub-dir
		}
    }
	
	closedir(dp);
		
    return NGX_OK;
}

static int save_ns_version(cycle_t *cycle)
{
    conf_server_t *sconf = (conf_server_t *)cycle->sconf;

	char v_name[PATH_LEN] = {0};
	string_xxsprintf((uchar_t *)v_name, "%s/current/VERSION", 
		sconf->fsimage_dir.data);
	
	int fd = open(v_name, O_RDWR | O_CREAT | O_TRUNC, 0664);
	if (fd < 0) 
	{
		printf("open %s err: %s\n", v_name, strerror(errno));
		
        return NGX_ERROR;
	}

	int64_t namespaceID = time_curtime();

	char ns_version[256] = {0};
	sprintf(ns_version, "namespaceID=%ld\n", namespaceID);

	if (write(fd, ns_version, strlen(ns_version)) < 0) 
    {
		printf("write %s err: %s\n", v_name, strerror(errno));

		return NGX_ERROR;
	}

	close(fd);
	
    return NGX_OK;
}

static int get_ns_version(cycle_t *cycle)
{
    conf_server_t *sconf = (conf_server_t *)cycle->sconf;

	char v_name[PATH_LEN] = {0};
	string_xxsprintf((uchar_t *)v_name, "%s/current/VERSION", 
		sconf->fsimage_dir.data);
	
	int fd = open(v_name, O_RDWR|O_CREAT,0777);
	if (fd < 0)   
	{
		dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno, 
			"open %s err, maybe use namenode -h , to see how to format dfs", v_name);
		
        return NGX_ERROR;
	}

    char ns_version[256] = {0};
	if (read(fd, ns_version, sizeof(ns_version)) < 0) 
    {
		dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
			"read %s err", v_name);

		close(fd);

		return NGX_ERROR;
	}

	sscanf(ns_version, "%*[^=]=%ld", &cycle->namespace_id);

	close(fd);
	
    return NGX_OK;
}

