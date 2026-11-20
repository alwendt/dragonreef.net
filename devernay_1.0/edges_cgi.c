#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <errno.h>
#include "devernay.h"

#define MAX_PIXELS 16000000
#define HEADER_SIZE 16

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

static unsigned int u32le(const unsigned char *p)
{
  return (unsigned int)p[0] | ((unsigned int)p[1] << 8) |
         ((unsigned int)p[2] << 16) | ((unsigned int)p[3] << 24);
}

static double query_number(const char *query, const char *name, double fallback)
{
  const char *p = query;
  size_t n = strlen(name);
  while(p != NULL && *p != '\0')
    {
      if((p == query || p[-1] == '&') && strncmp(p,name,n) == 0 && p[n] == '=')
        {
          char *end;
          double value;
          errno = 0;
          value = strtod(p+n+1,&end);
          if(errno == 0 && end != p+n+1) return value;
        }
      p = strchr(p,'&');
      if(p != NULL) ++p;
    }
  return fallback;
}

static void fail(const char *status, const char *message)
{
  printf("Status: %s\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n%s\n",
         status,message);
  exit(EXIT_SUCCESS);
}

static int source_index(double x, double y, int width, int height)
{
  int ix = (int)floor(x + 0.5);
  int iy = (int)floor(y + 0.5);
  if(ix < 0) ix = 0;
  if(iy < 0) iy = 0;
  if(ix >= width) ix = width - 1;
  if(iy >= height) iy = height - 1;
  return ix + iy * width;
}

int main(void)
{
  const char *method = getenv("REQUEST_METHOD");
  const char *length_text = getenv("CONTENT_LENGTH");
  const char *query = getenv("QUERY_STRING");
  unsigned char header[HEADER_SIZE];
  unsigned char *pixels;
  double *image, *x, *y, *gx, *gy, *magnitude;
  int *limits;
  int width, height, channels, point_count, curve_count;
  long length;
  size_t count, i;
  double sigma, low, high, stroke, max_magnitude = 0.0;

  if(method == NULL || strcmp(method,"POST") != 0)
    fail("405 Method Not Allowed","POST a PNG decoded by the /edges page.");
  if(length_text == NULL) fail("411 Length Required","Missing request length.");
  length = strtol(length_text,NULL,10);
  if(length < HEADER_SIZE || length > MAX_PIXELS * 4L + HEADER_SIZE)
    fail("413 Payload Too Large","Image is missing or too large (16 megapixels maximum).");
  if(fread(header,1,HEADER_SIZE,stdin) != HEADER_SIZE || memcmp(header,"EDGE",4) != 0)
    fail("400 Bad Request","Invalid edge image payload.");

  width = (int)u32le(header+4);
  height = (int)u32le(header+8);
  channels = (int)u32le(header+12);
  if(width < 5 || height < 5 || channels != 1 ||
     (long)width * height > MAX_PIXELS || length != HEADER_SIZE + (long)width * height)
    fail("400 Bad Request","Invalid image dimensions.");

  count = (size_t)width * (size_t)height;
  pixels = (unsigned char *)malloc(count);
  image = (double *)malloc(count * sizeof(double));
  if(pixels == NULL || image == NULL) fail("500 Internal Server Error","Out of memory.");
  if(fread(pixels,1,count,stdin) != count) fail("400 Bad Request","Incomplete image payload.");
  for(i=0; i<count; ++i) image[i] = pixels[i];
  free(pixels);

  if(query == NULL) query = "";
  sigma = query_number(query,"sigma",1.0);
  low = query_number(query,"low",5.0);
  high = query_number(query,"high",15.0);
  stroke = query_number(query,"stroke",1.25);
  if(sigma < 0.0 || sigma > 20.0 || low < 0.0 || high < low || high > 1000.0 ||
     stroke < 0.1 || stroke > 10.0)
    fail("400 Bad Request","Parameters are outside their allowed ranges.");

  devernay(&x,&y,&point_count,&limits,&curve_count,image,width,height,sigma,high,low);
  devernay_gradient(&gx,&gy,&magnitude,image,width,height,sigma);
  free(image);

  for(i=0; i<(size_t)point_count; ++i)
    {
      double m = magnitude[source_index(x[i],y[i],width,height)];
      if(m > max_magnitude) max_magnitude = m;
    }
  if(max_magnitude <= 0.0) max_magnitude = 1.0;

  printf("Content-Type: image/svg+xml\r\n");
  printf("Content-Disposition: inline; filename=edges.svg\r\n");
  printf("Cache-Control: no-store\r\n\r\n");
  printf("<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 %d %d\" width=\"%d\" height=\"%d\">\n",width,height,width,height);
  printf("<rect width=\"100%%\" height=\"100%%\" fill=\"#05070b\"/>\n");
  printf("<g fill=\"none\" stroke-linecap=\"round\" stroke-width=\"%.3g\">\n",stroke);
  {
    int curve;
    for(curve=0; curve<curve_count; ++curve)
      {
        int first = limits[curve];
        int end = limits[curve+1];
        int p;
        for(p=first; p+1<end; ++p)
          {
            int idx = source_index(x[p],y[p],width,height);
            double tangent = atan2(gy[idx],gx[idx]) + M_PI/2.0;
            double hue, value, lightness;
            while(tangent < 0.0) tangent += M_PI;
            while(tangent >= M_PI) tangent -= M_PI;
            hue = tangent * 360.0 / M_PI;
            value = magnitude[idx] / max_magnitude;
            if(value > 1.0) value = 1.0;
            lightness = 8.0 + 62.0 * sqrt(value);
            printf("<path d=\"M %.4g %.4g L %.4g %.4g\" stroke=\"hsl(%.2f 100%% %.2f%%)\"/>\n",
                   x[p],y[p],x[p+1],y[p+1],hue,lightness);
          }
      }
  }
  printf("</g><metadata>Devernay 1.0; sigma=%.4g low=%.4g high=%.4g; %d points in %d contours; max gradient=%.6g</metadata></svg>\n",
         sigma,low,high,point_count,curve_count,max_magnitude);

  free(x); free(y); free(limits); free(gx); free(gy); free(magnitude);
  return EXIT_SUCCESS;
}
