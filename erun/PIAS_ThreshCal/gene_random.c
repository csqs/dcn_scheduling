#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <pthread.h>

//#include "src/common/cdf.h"


#ifndef max
    #define max(a,b) ((a) > (b) ? (a) : (b))
#endif
#ifndef min
    #define min(a,b) ((a) < (b) ? (a) : (b))
#endif

#define TG_CDF_TABLE_ENTRY 32
struct CDF_Entry
{
    double value;
    double cdf;
};

/* CDF distribution */
struct CDF_Table
{
    struct CDF_Entry* entries;
    int num_entry;  //number of entries in CDF table
    int max_entry; //maximum number of entries in CDF table
    double min_cdf;    //min value of CDF (default 0)
    double max_cdf;    //max value of CDF (default 1)
};

/* Initialize a CDF distribution */
void init_CDF(struct CDF_Table* table);

/* Free resources of a CDF distribution */
void free_CDF(struct CDF_Table* table);

/* Get CDF distribution from a given file */
void load_CDF(struct CDF_Table* table, char *file_name);

/* Print CDF distribution information */
void print_CDF(struct CDF_Table* table);

/* Get average value of CDF distribution */
double avg_CDF(struct CDF_Table* table);

/* Generate a random value based on CDF distribution */
double gen_random_CDF(struct CDF_Table* table);

void init_CDF(struct CDF_Table* table)
{
    if(!table)
    return;
    
    table->entries = (struct CDF_Entry*)malloc(TG_CDF_TABLE_ENTRY * sizeof(struct CDF_Entry));
    table->num_entry = 0;
    table->max_entry = TG_CDF_TABLE_ENTRY;
    table->min_cdf = 0;
    table->max_cdf = 1;
    
    if (!(table->entries))
    perror("Error: malloc");
}

/* Free resources of a CDF distribution */
void free_CDF(struct CDF_Table* table)
{
    if (table)
    free(table->entries);
}

/* Get CDF distribution from a given file */
void load_CDF(struct CDF_Table* table, char *file_name)
{
    FILE *fd = NULL;
    char line[256] = {'\0'};
    struct CDF_Entry* e = NULL;
    int i = 0;
    
    if (!table)
    return;
    
    fd = fopen(file_name, "r");
    if (!fd)
    perror("Error: fopen");
    
    while (fgets(line, sizeof(line), fd) != NULL)
    {
        /* Resize entries */
        if (table->num_entry >= table->max_entry)
        {
            table->max_entry *= 2;
            e = (struct CDF_Entry*)malloc(table->max_entry * sizeof(struct CDF_Entry));
            if (!e)
            perror("Error: malloc");
            for (i = 0; i < table->num_entry; i++)
            e[i] = table->entries[i];
            free(table->entries);
            table->entries = e;
        }
        
        sscanf(line, "%lf %lf", &(table->entries[table->num_entry].value), &(table->entries[table->num_entry].cdf));
        
        if (table->min_cdf > table->entries[table->num_entry].cdf)
        table->min_cdf = table->entries[table->num_entry].cdf;
        if (table->max_cdf < table->entries[table->num_entry].cdf)
        table->max_cdf = table->entries[table->num_entry].cdf;
        
        table->num_entry++;
    }
    fclose(fd);
}

/* Print CDF distribution information */
void print_CDF(struct CDF_Table* table)
{
    int i = 0;
    
    if (!table)
    return;
    
    for (i = 0; i < table->num_entry; i++)
    printf("%.2f %.2f\n", table->entries[i].value, table->entries[i].cdf);
}

/* Get average value of CDF distribution */
double avg_CDF(struct CDF_Table* table)
{
    int i = 0;
    double avg = 0;
    double value, prob;
    
    if (!table)
    return 0;
    
    for (i = 0; i < table->num_entry; i++)
    {
        if (i == 0)
        {
            value = table->entries[i].value/2;
            prob = table->entries[i].cdf;
        }
        else
        {
            value = (table->entries[i].value + table->entries[i-1].value)/2;
            prob = table->entries[i].cdf - table->entries[i-1].cdf;
        }
        avg += (value * prob);
    }
    
    return avg;
}

double interpolate(double x, double x1, double y1, double x2, double y2)
{
    if (x1 == x2)
    return (y1 + y2) / 2;
    else
    return y1 + (x - x1) * (y2 - y1) / (x2 - x1);
}

/* generate a random floating point number from min to max */
double rand_range(double min, double max)
{
    return min + rand() * (max - min) / RAND_MAX;
}

/* Generate a random value based on CDF distribution */
double gen_random_CDF(struct CDF_Table* table)
{
    int i = 0;
    double x = rand_range(table->min_cdf, table->max_cdf);
    //printf("%f %f %f\n", x, table->min_cdf, table->max_cdf);
    
    if (!table)
    return 0;
    
    for (i = 0; i < table->num_entry; i++)
    {
        if (x <= table->entries[i].cdf)
        {
            if (i == 0)
            return interpolate(x, 0, 0, table->entries[i].cdf, table->entries[i].value);
            else
            return interpolate(x, table->entries[i-1].cdf, table->entries[i-1].value, table->entries[i].cdf, table->entries[i].value);
        }
    }
    
    return table->entries[table->num_entry-1].value;
}


int main(int argc, char *argv[])
{
    int num, total = 1000000, i = 0;
    struct CDF_Table* req_size_dist;
    char fct_log_name[80] = "flow_size_vl2.txt";
    FILE *fd = NULL;
    fd = fopen(fct_log_name, "w");
    
    req_size_dist = (struct CDF_Table*)malloc(sizeof(struct CDF_Table));
    printf("!!!");
    init_CDF(req_size_dist);
    load_CDF(req_size_dist, "random_vl2_cdf.txt");
    
    printf("Average request size: %.2f bytes\n", avg_CDF(req_size_dist));
    for(i = 0; i < total; i++){
        num = gen_random_CDF(req_size_dist);
        fprintf(fd, "%d\n", num);
    }
    fclose(fd);
        
    return 0;
           
}